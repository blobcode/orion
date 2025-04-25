#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <atomic>
#include <uv.h>
#include <lua5.4/lua.hpp>
#include "../build/src/task.pb.h"
#include <vector>
#include <mutex>

// config
const char *RUNNER_IP = "0.0.0.0";
const int RUNNER_PORT = 7000;
const char *WORKER_TARGET_IP = "127.0.0.1";

typedef struct
{
    char *base;
    size_t len;
} read_buf_t;

// Allocate buffer for libuv reads
void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    buf->base = (char *)malloc(suggested_size);
    if (!buf->base)
    {
        buf->len = 0; // allocation fails
        fprintf(stderr, "Runner/Worker: Failed to allocate buffer\n");
        return;
    }
    buf->len = suggested_size;
}

struct RunnerClient
{
    uv_tcp_t handle;
    std::string id;
    bool closing = false;
};

struct RunnerState
{
    std::vector<std::shared_ptr<RunnerClient>> workers;
    size_t next_worker_index = 0; // round robin index
    std::mutex workers_mutex;
    uv_loop_t *loop = nullptr;
    uv_tcp_t server_handle;
};

RunnerState runner_state;

void on_runner_read(uv_stream_t *client_stream, ssize_t nread, const uv_buf_t *buf);
void on_runner_close(uv_handle_t *handle); // Forward declaration

// Function to remove a worker from the list safely
void remove_worker(RunnerClient *client_to_remove)
{
    std::lock_guard<std::mutex> lock(runner_state.workers_mutex);

    runner_state.workers.erase(
        std::remove_if(runner_state.workers.begin(), runner_state.workers.end(),
                       [&](const std::shared_ptr<RunnerClient> &worker_ptr)
                       {
                           return worker_ptr.get() == client_to_remove;
                       }),
        runner_state.workers.end());

    // Adjust round-robin index if it's now out of bounds
    if (!runner_state.workers.empty() && runner_state.next_worker_index >= runner_state.workers.size())
    {
        runner_state.next_worker_index = 0; // Reset to start
    }
    else if (runner_state.workers.empty())
    {
        runner_state.next_worker_index = 0; // Reset if empty
    }
    std::cout << "Runner: Worker removed. Remaining workers: " << runner_state.workers.size() << std::endl;
}

void on_new_connection(uv_stream_t *server, int status)
{
    if (status < 0)
    {
        fprintf(stderr, "New connection error %s\n", uv_strerror(status));
        return;
    }

    // Ensure server handle matches our runner state server handle
    if (server != (uv_stream_t *)&runner_state.server_handle)
    {
        fprintf(stderr, "Runner: Mismatched server handle in on_new_connection\n");
        return;
    }

    auto client_info = std::make_shared<RunnerClient>();
    // Initialize the handle within the Runner's loop
    if (uv_tcp_init(runner_state.loop, &client_info->handle) != 0)
    {
        fprintf(stderr, "Runner: Failed to initialize client tcp handle.\n");
        // Cannot proceed with this client
        return;
    }
    client_info->handle.data = client_info.get(); // Link back to our struct

    if (uv_accept(server, (uv_stream_t *)&client_info->handle) == 0)
    {
        std::cout << "Runner: Worker connected." << std::endl;
        // Add worker to the list under lock
        {
            std::lock_guard<std::mutex> lock(runner_state.workers_mutex);
            runner_state.workers.push_back(client_info);
            std::cout << "Runner: Worker added. Total workers: " << runner_state.workers.size() << std::endl;
        }
        // Start reading data from the worker (e.g., registration message)
        uv_read_start((uv_stream_t *)&client_info->handle, alloc_buffer, on_runner_read);
    }
    else
    {
        fprintf(stderr, "Runner: Failed to accept worker connection.\n");
        // Close the handle if accept failed
        // Use uv_close and provide a callback, even if simple
        uv_close((uv_handle_t *)&client_info->handle, [](uv_handle_t *handle)
                 {
            // Basic cleanup callback for failed accept
            // The shared_ptr will manage the RunnerClient memory if it goes out of scope
             std::cout << "Runner: Closed handle for failed accept." << std::endl; });
    }
}

void on_runner_read(uv_stream_t *client_stream, ssize_t nread, const uv_buf_t *buf)
{
    RunnerClient *client_info = static_cast<RunnerClient *>(client_stream->data);
    if (!client_info || client_info->closing)
    {
        if (buf->base)
            free(buf->base); // Free buffer even if client is closing
        return;              // Ignore reads if client is closing or invalid
    }

    if (nread > 0)
    {
        std::cout << "Runner: Received " << nread << " bytes from worker "
                  << (client_info->id.empty() ? "[unknown]" : client_info->id) << std::endl;

        TaskResult result_msg;
        WorkerRegister register_msg; // Try parsing as register first

        if (register_msg.ParseFromArray(buf->base, nread))
        {
            // handle register
            if (client_info->id.empty())
            { // Only register once
                client_info->id = register_msg.worker_id();
                std::cout << "Runner: Worker registered with ID: " << client_info->id << std::endl;
            }
        }
        else if (result_msg.ParseFromArray(buf->base, nread))
        {
            // task result
            std::cout << "Runner: Received result for task " << result_msg.task_id()
                      << " from worker " << client_info->id
                      << ", Success: " << result_msg.success() << std::endl;
            if (!result_msg.success())
            {
                std::cerr << "Runner: Task Error from " << client_info->id << ": " << result_msg.error_message() << std::endl;
            }
            // TODO: handle result
        }
        else
        {
            std::cerr << "Runner: Failed to parse message from worker "
                      << (client_info->id.empty() ? "[unknown]" : client_info->id) << "." << std::endl;
        }
    }
    else if (nread < 0)
    {
        if (nread != UV_EOF)
        {
            fprintf(stderr, "Runner: Read error on worker %s: %s\n",
                    (client_info->id.empty() ? "[unknown]" : client_info->id.c_str()),
                    uv_err_name(nread));
        }
        else
        {
            std::cout << "Runner: Worker " << (client_info->id.empty() ? "[unknown]" : client_info->id) << " sent EOF." << std::endl;
        }
        client_info->closing = true;
        uv_close((uv_handle_t *)client_stream, on_runner_close);
    }

    if (buf->base)
    {
        free(buf->base);
    }
}

void on_runner_close(uv_handle_t *handle)
{
    RunnerClient *client_info = static_cast<RunnerClient *>(handle->data);
    if (!client_info)
        return;

    std::cout << "Runner: Worker " << (client_info->id.empty() ? "[unknown]" : client_info->id) << " connection fully closed." << std::endl;

    remove_worker(client_info);
}
struct WriteRequestData
{
    uv_write_t req;
    uv_buf_t buf;
    std::string data_buffer; // Keep the serialized data alive

    // Prevent copying
    WriteRequestData(const WriteRequestData &) = delete;
    WriteRequestData &operator=(const WriteRequestData &) = delete;

    // Constructor to initialize
    WriteRequestData(const std::string &data) : data_buffer(data)
    {
        // Ensure the uv_buf_t points to the string's data
        buf = uv_buf_init((char *)data_buffer.data(), data_buffer.length());
        req.data = this; // Point back to this structure
    }
};

// Function to send a task to a specific worker
void send_task_to_worker(RunnerClient *worker, const std::string &task_id, const std::string &script)
{
    if (!worker || worker->closing)
    {
        std::cerr << "Runner: Attempted to send task to invalid or closing worker." << std::endl;
        return;
    }

    TaskRequest task_msg;
    task_msg.set_task_id(task_id);
    task_msg.set_script(script);
    // task_msg.set_input_data(...); // Optional input data

    std::string serialized_task;
    if (!task_msg.SerializeToString(&serialized_task))
    {
        std::cerr << "Runner: Failed to serialize task " << task_id << "." << std::endl;
        return;
    }

    // TODO: Implement proper message framing (send size first)
    // For now, just sending raw protobuf data

    // Create write request data on the heap, managed by the callback
    WriteRequestData *write_data = new WriteRequestData(serialized_task);

    std::cout << "Runner: Sending task " << task_id << " to worker " << worker->id << std::endl;

    // Perform the write operation
    int write_status = uv_write(&write_data->req, // Use req from write_data
                                (uv_stream_t *)&worker->handle,
                                &write_data->buf, // Use buf from write_data
                                1,
                                [](uv_write_t *req, int status)
                                {
                                    // Get our data structure back
                                    WriteRequestData *completed_write_data = static_cast<WriteRequestData *>(req->data);

                                    if (status)
                                    {
                                        fprintf(stderr, "Runner: Write error %s\n", uv_strerror(status));
                                        // Handle write error, maybe close connection?
                                        RunnerClient *client_info = static_cast<RunnerClient *>(req->handle->data);
                                        if (client_info && !client_info->closing)
                                        {
                                            client_info->closing = true;
                                            uv_close((uv_handle_t *)req->handle, on_runner_close);
                                        }
                                    }
                                    else
                                    {
                                        // std::cout << "Runner: Task sent successfully (write callback)." << std::endl;
                                        //  Write successful, nothing specific to do here for now
                                    }

                                    // Clean up the WriteRequestData structure now that the write is done
                                    delete completed_write_data;
                                });

    if (write_status != 0)
    {
        fprintf(stderr, "Runner: uv_write failed immediately for task %s: %s\n", task_id.c_str(), uv_strerror(write_status));
        // Clean up the allocated data if uv_write failed instantly
        delete write_data;
        // Potentially close the connection here too
        if (!worker->closing)
        {
            worker->closing = true;
            uv_close((uv_handle_t *)&worker->handle, on_runner_close);
        }
    }
}

int run_runner()
{
    runner_state.loop = uv_default_loop();
    if (!runner_state.loop)
    {
        fprintf(stderr, "Runner: Failed to get default loop.\n");
        return 1;
    }

    // Use the server handle from runner_state
    uv_tcp_init(runner_state.loop, &runner_state.server_handle);
    runner_state.server_handle.data = &runner_state; // Optional: link back to state

    struct sockaddr_in bind_addr;
    uv_ip4_addr(RUNNER_IP, RUNNER_PORT, &bind_addr);

    int bind_ret = uv_tcp_bind(&runner_state.server_handle, (const struct sockaddr *)&bind_addr, 0);
    if (bind_ret)
    {
        fprintf(stderr, "Runner: Bind error %s\n", uv_strerror(bind_ret));
        uv_close((uv_handle_t *)&runner_state.server_handle, nullptr); // Close handle on error
        return 1;
    }

    int listen_ret = uv_listen((uv_stream_t *)&runner_state.server_handle, 128, on_new_connection); // 128 is backlog size
    if (listen_ret)
    {
        fprintf(stderr, "Runner: Listen error %s\n", uv_strerror(listen_ret));
        uv_close((uv_handle_t *)&runner_state.server_handle, nullptr); // Close handle on error
        return 1;
    }
    std::cout << "Runner: Listening on " << RUNNER_IP << ":" << RUNNER_PORT << std::endl;

    // sample sending tasks
    // TODO: task queue
    uv_timer_t task_timer;
    uv_timer_init(runner_state.loop, &task_timer);
    task_timer.data = &runner_state;

    uv_timer_start(&task_timer, [](uv_timer_t *handle)
                   {
        RunnerState* state = static_cast<RunnerState*>(handle->data);
        std::lock_guard<std::mutex> lock(state->workers_mutex);

        if (!state->workers.empty()) {
            // round robin scheduler
            if (state->next_worker_index >= state->workers.size()) {
                state->next_worker_index = 0; 
            }

            std::shared_ptr<RunnerClient> worker = state->workers[state->next_worker_index];

            if (worker && !worker->closing) {
                static int task_counter = 0;
                std::string task_id = "task_" + std::to_string(++task_counter);
                std::string script = "print('Hello from Lua task " + task_id + " on worker " + worker->id + "!') return 'Result for " + task_id + "'";
                send_task_to_worker(worker.get(), task_id, script);

                state->next_worker_index = (state->next_worker_index + 1) % state->workers.size();
            }

        } else {
            std::cout << "no workers available" << std::endl;
        } }, 5000, 10); // start after 5s

    std::cout << "Runner: Starting event loop..." << std::endl;
    int run_ret = uv_run(runner_state.loop, UV_RUN_DEFAULT);
    std::cout << "Runner: Event loop finished with code " << run_ret << std::endl;

    // cleanup
    uv_close((uv_handle_t *)&runner_state.server_handle, nullptr);
    uv_run(runner_state.loop, UV_RUN_ONCE);

    return run_ret;
}

// worker stuff
uv_loop_t *worker_loop;
uv_tcp_t worker_socket;
uv_connect_t worker_connect_req;
lua_State *L; // Lua state
std::string worker_id_str;

void on_worker_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
void on_worker_close(uv_handle_t *handle);

void send_result(uv_stream_t *stream, const TaskResult &result_msg)
{
    std::string serialized_result;
    if (!result_msg.SerializeToString(&serialized_result))
    {
        std::cerr << "Worker [" << worker_id_str << "]: Failed to serialize result." << std::endl;
        return;
    }

    // TODO: framing

    WriteRequestData *write_data = new WriteRequestData(serialized_result);

    int write_status = uv_write(&write_data->req,
                                stream,
                                &write_data->buf,
                                1,
                                [](uv_write_t *req, int status)
                                {
                                    WriteRequestData *completed_write_data = static_cast<WriteRequestData *>(req->data);

                                    // write error
                                    if (status)
                                    {
                                        fprintf(stderr, "Worker: Write error sending result: %s\n", uv_strerror(status));
                                        uv_close((uv_handle_t *)req->handle, on_worker_close);
                                    }
                                    delete completed_write_data;
                                });

    if (write_status != 0)
    {
        fprintf(stderr, "Worker: uv_write failed immediately for result: %s\n", uv_strerror(write_status));
        delete write_data;
        uv_close((uv_handle_t *)stream, on_worker_close); // close on error?
    }
}

TaskResult execute_lua_task(const std::string &script)
{
    TaskResult result;
    result.set_success(false);

    if (!L)
    {
        result.set_error_message("Lua state not initialized.");
        return result;
    }

    // Load and execute the script string
    int status = luaL_loadstring(L, script.c_str());
    if (status != LUA_OK)
    {
        const char *lua_err = lua_tostring(L, -1);
        result.set_error_message("Lua load error: " + std::string(lua_err ? lua_err : "Unknown"));
        lua_pop(L, 1);
        return result;
    }

    status = lua_pcall(L, 0, 1, 0);
    if (status != LUA_OK)
    {
        const char *lua_err = lua_tostring(L, -1);
        result.set_error_message("Lua execution error: " + std::string(lua_err ? lua_err : "Unknown"));
        lua_pop(L, 1); // Pop error message
        return result;
    }

    // check return value
    if (lua_gettop(L) >= 1)
    {
        if (lua_isstring(L, -1))
        {
            size_t len;
            const char *lua_res = lua_tolstring(L, -1, &len); // Use tolstring for length
            result.set_output(lua_res ? lua_res : "", len);   // Set result data
        }
        else
        {
            // TODO: Handle diff. return types
            result.set_error_message("Lua script returned non-string value.");
            lua_pop(L, 1);
            return result;
        }
        lua_pop(L, 1);
    }

    result.set_success(true);
    return result;
}

void on_worker_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    if (nread > 0)
    {

        // TODO: framing

        TaskRequest task_msg;
        if (task_msg.ParseFromArray(buf->base, nread))
        {
            std::cout << "Worker [" << worker_id_str << "]: Received task " << task_msg.task_id() << std::endl;

            // execution
            TaskResult result = execute_lua_task(task_msg.script());
            result.set_task_id(task_msg.task_id());
            send_result(stream, result);
        }
        else
        {
            std::cerr << "Worker [" << worker_id_str << "]: Failed to parse message from runner." << std::endl;
            // Close connection on parse failure?
            uv_close((uv_handle_t *)stream, on_worker_close);
        }
    }
    else if (nread < 0)
    {
        if (nread != UV_EOF)
        {
            fprintf(stderr, "Worker [%s]: Read error %s\n", worker_id_str.c_str(), uv_err_name(nread));
        }
        uv_close((uv_handle_t *)stream, on_worker_close);
    }

    if (buf->base)
    {
        free(buf->base);
    }
}

void on_worker_close(uv_handle_t *handle)
{
    std::cout << "Worker [" << worker_id_str << "]: Connection closed." << std::endl;
    if (L)
    {
        lua_close(L);
        L = nullptr;
        std::cout << "Worker [" << worker_id_str << "]: Lua state closed." << std::endl;
    }
}

void on_worker_connect(uv_connect_t *req, int status)
{
    if (status < 0)
    {
        fprintf(stderr, "Worker: Connection error %s\n", uv_strerror(status));
        // TODO: retry
        on_worker_close((uv_handle_t *)&worker_socket);
        return;
    }

    std::cout << "Worker: Connected to runner." << std::endl;

    WorkerRegister register_msg;

    char hostname[256];
    size_t size = sizeof(hostname);
    if (uv_os_gethostname(hostname, &size) != 0)
    {
        strncpy(hostname, "unknown_host", sizeof(hostname) - 1);
        hostname[sizeof(hostname) - 1] = '\0';
    }
    worker_id_str = std::string(hostname) + ":" + std::to_string(uv_os_getpid());
    register_msg.set_worker_id(worker_id_str);

    std::string serialized_reg;
    if (!register_msg.SerializeToString(&serialized_reg))
    {
        std::cerr << "Worker [" << worker_id_str << "]: Failed to serialize registration." << std::endl;
        uv_close((uv_handle_t *)req->handle, on_worker_close); // Close if cannot register
    }
    else
    {
        WriteRequestData *write_data = new WriteRequestData(serialized_reg);

        int write_status = uv_write(&write_data->req,
                                    req->handle,
                                    &write_data->buf,
                                    1,
                                    [](uv_write_t *wreq, int wstatus)
                                    {
                                        WriteRequestData *completed_write_data = static_cast<WriteRequestData *>(wreq->data);
                                        if (wstatus)
                                        {
                                            fprintf(stderr, "Worker: Registration write error %s\n", uv_strerror(wstatus));
                                            uv_close((uv_handle_t *)wreq->handle, on_worker_close); // Close on error
                                        }
                                        else
                                        {
                                            std::cout << "Worker [" << worker_id_str << "]: Registration sent." << std::endl;
                                            // read after registration
                                            uv_read_start(wreq->handle, alloc_buffer, on_worker_read);
                                        }
                                        delete completed_write_data; // clean up
                                    });

        if (write_status != 0)
        {
            fprintf(stderr, "Worker: uv_write failed immediately for registration: %s\n", uv_strerror(write_status));
            delete write_data;
            uv_close((uv_handle_t *)req->handle, on_worker_close);
        }
    }
}

int run_worker()
{
    worker_loop = uv_default_loop();
    if (!worker_loop)
    {
        fprintf(stderr, "Worker: Failed to get default loop.\n");
        return 1;
    }

    // Initialize Lua state
    L = luaL_newstate();
    if (!L)
    {
        std::cerr << "Worker: Failed to create Lua state." << std::endl;
        return 1;
    }
    luaL_openlibs(L);

    uv_tcp_init(worker_loop, &worker_socket);
    worker_socket.data = nullptr;

    struct sockaddr_in dest_addr;
    uv_ip4_addr(WORKER_TARGET_IP, RUNNER_PORT, &dest_addr);

    std::cout << "Worker: Attempting to connect to " << WORKER_TARGET_IP << ":" << RUNNER_PORT << std::endl;
    uv_tcp_connect(&worker_connect_req, &worker_socket, (const struct sockaddr *)&dest_addr, on_worker_connect);

    int ret = uv_run(worker_loop, UV_RUN_DEFAULT);
    std::cout << "Worker [" << worker_id_str << "]: Event loop finished." << std::endl;

    if (L)
    {
        lua_close(L);
        L = nullptr;
    }

    return ret;
}

int main(int argc, char **argv)
{
    if (argc > 1 && std::string(argv[1]) == "--worker")
    {
        std::cout << "starting worker..." << std::endl;
        GOOGLE_PROTOBUF_VERIFY_VERSION;
        int ret = run_worker();
        google::protobuf::ShutdownProtobufLibrary();
        return ret;
    }
    else
    {
        std::cout << "starting task runner..." << std::endl;
        // Initialize Protobuf library
        GOOGLE_PROTOBUF_VERIFY_VERSION;
        int ret = run_runner();
        google::protobuf::ShutdownProtobufLibrary();
        return ret;
    }
}
