#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <lua5.4/lua.hpp>

#include "task.pb.h"

// Structure for pending tasks
struct PendingTask
{
    std::string task_id;
    std::string script;
    std::string args;
    struct bufferevent *client_bev; // To send response back to client
};

// Structure for worker information
struct Worker
{
    std::string id;
    struct bufferevent *bev;
    bool busy;
};

// Task Queue
class TaskQueue
{
public:
    void pushTask(const PendingTask &task)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(task);
        cv_.notify_one();
    }

    bool waitAndPopTask(PendingTask &task)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]
                 { return !tasks_.empty(); });
        task = tasks_.front();
        tasks_.pop();
        return true;
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

private:
    std::queue<PendingTask> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

// Worker Manager
class WorkerManager
{
public:
    void addWorker(const std::string &id, struct bufferevent *bev)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        workers_[id] = {id, bev, false};
        std::cout << "Worker " << id << " connected" << std::endl;
    }

    void removeWorker(const std::string &id)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it != workers_.end())
        {
            bufferevent_free(it->second.bev);
            std::cout << "Worker " << id << " disconnected" << std::endl;
            workers_.erase(it);
        }
    }

    void removeWorker(struct bufferevent *bev)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = workers_.begin(); it != workers_.end(); ++it)
        {
            if (it->second.bev == bev)
            {
                std::cout << "Worker " << it->first << " disconnected" << std::endl;
                workers_.erase(it);
                return;
            }
        }
    }

    bool getAvailableWorker(Worker &worker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &pair : workers_)
        {
            if (!pair.second.busy)
            {
                worker = pair.second;
                pair.second.busy = true;
                return true;
            }
        }
        return false;
    }

    void markWorkerAvailable(const std::string &id)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it != workers_.end())
        {
            it->second.busy = false;
        }
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return workers_.size();
    }

    bool hasAvailableWorker()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &pair : workers_)
        {
            if (!pair.second.busy)
                return true;
        }
        return false;
    }

    // Check if a bev is already registered
    bool isRegisteredBev(struct bufferevent *bev)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &pair : workers_)
        {
            if (pair.second.bev == bev)
                return true;
        }
        return false;
    }

private:
    std::map<std::string, Worker> workers_;
    std::mutex mutex_;
};

// Broker server context
struct BrokerContext
{
    TaskQueue taskQueue;
    WorkerManager workerManager;
    struct event_base *base;
};

// Task service for local execution (used by workers)
class TaskService
{
public:
    TaskService()
    {
        // Initialize Lua state
        L = luaL_newstate();
        luaL_openlibs(L);
    }

    ~TaskService()
    {
        if (L)
            lua_close(L);
    }

    // Execute a Lua function with the given script and arguments
    std::string executeLuaTask(const std::string &script, const std::string &args)
    {
        std::string result;

        // Load and run the script
        if (luaL_dostring(L, script.c_str()) != 0)
        {
            result = "Error loading script: " + std::string(lua_tostring(L, -1));
            lua_pop(L, 1);
            return result;
        }

        // Check if 'execute' function exists
        lua_getglobal(L, "execute");
        if (!lua_isfunction(L, -1))
        {
            lua_pop(L, 1);
            return "Error: execute function not found in script";
        }

        // Push arguments and call function
        lua_pushstring(L, args.c_str());
        if (lua_pcall(L, 1, 1, 0) != 0)
        {
            result = "Error executing function: " + std::string(lua_tostring(L, -1));
            lua_pop(L, 1);
            return result;
        }

        // Get the result
        if (lua_isstring(L, -1))
        {
            result = lua_tostring(L, -1);
        }
        else
        {
            // Convert non-string results to string representation if possible, or indicate type
            int type = lua_type(L, -1);
            result = "Function returned a non-string result (Type: " + std::string(lua_typename(L, type)) + ")";
        }

        lua_pop(L, 1);
        return result;
    }

private:
    lua_State *L;
};

// Dispatcher thread to match tasks with workers
void dispatcherThread(BrokerContext *ctx)
{
    while (true)
    {
        // Wait for an available task
        PendingTask task;
        ctx->taskQueue.waitAndPopTask(task);

        // Try to find an available worker
        Worker worker;
        bool foundWorker = false;

        // Retry until a worker is available or all workers disconnect
        while (!foundWorker)
        {
            foundWorker = ctx->workerManager.getAvailableWorker(worker);
            if (!foundWorker)
            {
                // If no workers are available, wait a bit and try again
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

                // If still no workers, put task back in queue and break
                if (ctx->workerManager.size() == 0)
                {
                    ctx->taskQueue.pushTask(task); // Put task back for later processing
                    std::cout << "No workers available, task " << task.task_id << " requeued." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(1)); // Prevent busy waiting
                    break;                                                // Exit the inner loop to process the next task
                }
            }
        }

        if (foundWorker)
        {
            // Create a task message for the worker
            task::TaskRequest request;
            request.set_task_id(task.task_id);
            request.set_script(task.script);
            request.set_args(task.args);

            // Add client information to the request for routing response
            request.set_client_id(std::to_string(reinterpret_cast<uintptr_t>(task.client_bev)));

            // Serialize the request
            std::string requestStr;
            if (!request.SerializeToString(&requestStr))
            {
                std::cerr << "Failed to serialize worker request for task " << task.task_id << std::endl;
                ctx->workerManager.markWorkerAvailable(worker.id); // Mark worker available on serialization failure
                continue;
            }

            // Send the size first
            uint32_t requestSize = htonl(requestStr.size());
            if (bufferevent_write(worker.bev, &requestSize, sizeof(requestSize)) < 0)
            {
                std::cerr << "Failed to write task size to worker " << worker.id << std::endl;
                ctx->workerManager.markWorkerAvailable(worker.id); // Mark worker available on write failure
                continue;
            }

            // Send the request
            if (bufferevent_write(worker.bev, requestStr.c_str(), requestStr.size()) < 0)
            {
                std::cerr << "Failed to write task data to worker " << worker.id << std::endl;
                ctx->workerManager.markWorkerAvailable(worker.id); // Mark worker available on write failure
                continue;
            }

            std::cout << "Task " << task.task_id << " assigned to worker " << worker.id << std::endl;
        }
    }
}

// Client read callback for the broker server
static void brokerClientReadCallback(struct bufferevent *bev, void *ctx)
{
    BrokerContext *brokerCtx = static_cast<BrokerContext *>(ctx);
    struct evbuffer *input = bufferevent_get_input(bev);

    while (true)
    {
        // Read message size
        uint32_t msgSize;
        if (evbuffer_copyout(input, &msgSize, sizeof(msgSize)) < sizeof(msgSize))
        {
            return; // Not enough data yet for size
        }

        msgSize = ntohl(msgSize);

        // Check if we have the complete message
        if (evbuffer_get_length(input) < sizeof(msgSize) + msgSize)
        {
            return; // Wait for more data
        }

        // Remove the size field
        evbuffer_drain(input, sizeof(msgSize));

        // Read the protobuf message
        std::unique_ptr<char[]> data(new char[msgSize]);
        evbuffer_remove(input, data.get(), msgSize);

        // Parse the task request
        task::TaskRequest request;
        if (!request.ParseFromArray(data.get(), msgSize))
        {
            std::cerr << "Failed to parse client request" << std::endl;
            // Continue processing in case there's more valid data
            continue;
        }

        // Add the task to the queue
        PendingTask task;
        task.task_id = request.task_id();
        task.script = request.script();
        task.args = request.args();
        task.client_bev = bev; // Store the client's bufferevent

        brokerCtx->taskQueue.pushTask(task);

        std::cout << "Task " << task.task_id << " received from client, queued for processing" << std::endl;
    }
}

static void brokerWorkerReadCallback(struct bufferevent *bev, void *ctx)
{
    BrokerContext *brokerCtx = static_cast<BrokerContext *>(ctx);
    struct evbuffer *input = bufferevent_get_input(bev);

    while (true)
    {
        uint32_t msgSize;
        if (evbuffer_copyout(input, &msgSize, sizeof(msgSize)) < sizeof(msgSize))
            return;
        msgSize = ntohl(msgSize);
        if (evbuffer_get_length(input) < sizeof(msgSize) + msgSize)
            return;
        evbuffer_drain(input, sizeof(msgSize));

        std::unique_ptr<char[]> data(new char[msgSize]);
        evbuffer_remove(input, data.get(), msgSize);

        bool registered = brokerCtx->workerManager.isRegisteredBev(bev);

        if (!registered)
        {
            // Expecting WorkerRegistration
            task::WorkerRegistration reg;
            if (reg.ParseFromArray(data.get(), msgSize))
            {
                brokerCtx->workerManager.addWorker(reg.worker_id(), bev);

                // Send acknowledgment
                task::WorkerAcknowledgment ack;
                ack.set_worker_id(reg.worker_id());
                ack.set_status("registered");
                std::string ackStr;
                if (ack.SerializeToString(&ackStr))
                {
                    uint32_t ackSize = htonl(ackStr.size());
                    bufferevent_write(bev, &ackSize, sizeof(ackSize));
                    bufferevent_write(bev, ackStr.c_str(), ackStr.size());
                }
            }
            else
            {
                std::cerr << "Invalid registration from worker" << std::endl;
            }
        }
        else
        {
            // Handle TaskResponse
            task::TaskResponse response;
            if (response.ParseFromArray(data.get(), msgSize))
            {
                // Mark worker available again
                std::string workerId = response.worker_id();
                brokerCtx->workerManager.markWorkerAvailable(workerId);

                // Forward to client
                uintptr_t clientId = std::stoull(response.client_id());
                struct bufferevent *clientBev = reinterpret_cast<struct bufferevent *>(clientId);

                task::TaskResponse clientResp;
                clientResp.set_task_id(response.task_id());
                clientResp.set_result(response.result());
                std::string respStr;
                if (clientResp.SerializeToString(&respStr))
                {
                    uint32_t respSize = htonl(respStr.size());
                    bufferevent_write(clientBev, &respSize, sizeof(respSize));
                    bufferevent_write(clientBev, respStr.c_str(), respStr.size());
                }
                std::cout << "Task " << response.task_id() << " completed by worker "
                          << workerId << ", result sent to client" << std::endl;
            }
            else
            {
                std::cerr << "Unknown message type from worker" << std::endl;
            }
        }
    }
}
// Event callback for clients and workers
static void brokerEventCallback(struct bufferevent *bev, short events, void *ctx)
{
    BrokerContext *brokerCtx = static_cast<BrokerContext *>(ctx);

    if (events & BEV_EVENT_ERROR)
    {
        int err = EVUTIL_SOCKET_ERROR();
        std::cerr << "Error from bufferevent: " << evutil_socket_error_to_string(err) << std::endl;
    }

    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR))
    {
        brokerCtx->workerManager.removeWorker(bev); // Attempt to remove as worker
        // If not a worker, it's likely a client, and bufferevent_free handles cleanup.
        bufferevent_free(bev);
    }
}

// Accept callback for the broker server (for clients)
static void brokerAcceptCallback(struct evconnlistener *listener, evutil_socket_t fd,
                                 struct sockaddr *address, int socklen, void *ctx)
{
    BrokerContext *brokerCtx = static_cast<BrokerContext *>(ctx);
    struct event_base *base = evconnlistener_get_base(listener);

    // Set up the bufferevent for the new connection
    struct bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!bev)
    {
        std::cerr << "Error constructing bufferevent for client connection" << std::endl;
        evutil_closesocket(fd);
        return;
    }

    // This is a client connection. Set the client read callback.
    bufferevent_setcb(bev, brokerClientReadCallback, NULL, brokerEventCallback, brokerCtx);
    bufferevent_enable(bev, EV_READ | EV_WRITE);

    std::cout << "Accepted client connection on fd " << fd << std::endl;
}

// Accept callback for worker registration
static void workerAcceptCallback(struct evconnlistener *listener, evutil_socket_t fd,
                                 struct sockaddr *address, int socklen, void *ctx)
{
    BrokerContext *brokerCtx = static_cast<BrokerContext *>(ctx);
    struct event_base *base = evconnlistener_get_base(listener);

    // Set up the bufferevent for the new connection
    struct bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!bev)
    {
        std::cerr << "Error constructing bufferevent for worker connection" << std::endl;
        evutil_closesocket(fd);
        return;
    }

    // This is a worker connection. Set the worker read callback.
    bufferevent_setcb(bev, brokerWorkerReadCallback, NULL, brokerEventCallback, brokerCtx);
    bufferevent_enable(bev, EV_READ | EV_WRITE);

    std::cout << "Accepted worker connection" << std::endl;
}

// Worker context for task execution
struct WorkerContext
{
    std::string worker_id;
    TaskService service;
    struct event_base *base;
    struct bufferevent *broker_bev;
    bool registered{false};
};

// Read callback for worker
static void workerReadCallback(struct bufferevent *bev, void *ctx)
{
    WorkerContext *workerCtx = static_cast<WorkerContext *>(ctx);
    struct evbuffer *input = bufferevent_get_input(bev);

    while (true)
    {
        // 1) Read the 4-byte size prefix
        uint32_t netMsgSize;
        if (evbuffer_copyout(input, &netMsgSize, sizeof(netMsgSize)) < sizeof(netMsgSize))
            return; // not enough data yet
        uint32_t msgSize = ntohl(netMsgSize);

        // 2) Wait until the full payload is available
        if (evbuffer_get_length(input) < sizeof(netMsgSize) + msgSize)
            return;
        evbuffer_drain(input, sizeof(netMsgSize));

        // 3) Extract the payload
        std::unique_ptr<char[]> data(new char[msgSize]);
        evbuffer_remove(input, data.get(), msgSize);

        if (!workerCtx->registered)
        {
            // -- Registration handshake --
            task::WorkerAcknowledgment ack;
            if (ack.ParseFromArray(data.get(), msgSize))
            {
                workerCtx->registered = true;
                std::cout << "Worker " << ack.worker_id()
                          << " status: " << ack.status() << std::endl;
            }
            else
            {
                std::cerr << "Expected WorkerAcknowledgment but failed to parse." << std::endl;
            }
        }
        else
        {
            // -- Actual task dispatch --
            task::TaskRequest request;
            if (request.ParseFromArray(data.get(), msgSize))
            {
                std::cout << "Worker " << workerCtx->worker_id
                          << " received task " << request.task_id() << std::endl;

                // Execute the Lua script
                std::string result = workerCtx->service.executeLuaTask(
                    request.script(), request.args());

                // Build response
                task::TaskResponse response;
                response.set_task_id(request.task_id());
                response.set_result(result);
                response.set_worker_id(workerCtx->worker_id);
                response.set_client_id(request.client_id());

                std::string out;
                if (!response.SerializeToString(&out))
                {
                    std::cerr << "Failed to serialize TaskResponse for task "
                              << request.task_id() << std::endl;
                    return;
                }

                // Send size + payload back to broker
                uint32_t outSize = htonl(out.size());
                if (bufferevent_write(bev, &outSize, sizeof(outSize)) < 0 ||
                    bufferevent_write(bev, out.c_str(), out.size()) < 0)
                {
                    std::cerr << "Failed to send TaskResponse for task "
                              << request.task_id() << std::endl;
                }
                else
                {
                    std::cout << "Worker " << workerCtx->worker_id
                              << " sent result for task " << request.task_id() << std::endl;
                }
            }
            else
            {
                std::cerr << "Expected TaskRequest after registration but failed to parse." << std::endl;
            }
        }
    }
}

// Event callback for worker
static void workerEventCallback(struct bufferevent *bev, short events, void *ctx)
{
    WorkerContext *workerCtx = static_cast<WorkerContext *>(ctx);

    if (events & BEV_EVENT_ERROR)
    {
        int err = EVUTIL_SOCKET_ERROR();
        std::cerr << "Error from bufferevent: " << evutil_socket_error_to_string(err) << std::endl;
    }
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT))
    {
        std::cerr << "Connection to broker closed. Exiting worker." << std::endl;
        event_base_loopexit(bufferevent_get_base(bev), NULL); // Exit the worker's event loop
    }
    if (events & BEV_EVENT_CONNECTED)
    {
        std::cout << "Worker connected successfully to broker." << std::endl;

        // === ADD THIS SECTION ===
        // Send Worker Registration message
        task::WorkerRegistration reg;
        reg.set_worker_id(workerCtx->worker_id); // Use the worker's ID

        std::string regStr;
        if (!reg.SerializeToString(&regStr))
        {
            std::cerr << "Failed to serialize worker registration message" << std::endl;
            // Handle error, possibly disconnect
            bufferevent_free(bev);
            event_base_loopexit(bufferevent_get_base(bev), NULL);
            return;
        }

        // Send the size first
        uint32_t regSize = htonl(regStr.size());
        if (bufferevent_write(bev, &regSize, sizeof(regSize)) < 0)
        {
            std::cerr << "Failed to write registration size to broker." << std::endl;
            // Handle error, possibly disconnect
            bufferevent_free(bev);
            event_base_loopexit(bufferevent_get_base(bev), NULL);
            return;
        }

        // Send the registration message
        if (bufferevent_write(bev, regStr.c_str(), regStr.size()) < 0)
        {
            std::cerr << "Failed to write registration data to broker." << std::endl;
            // Handle error, possibly disconnect
            bufferevent_free(bev);
            event_base_loopexit(bufferevent_get_base(bev), NULL);
            return;
        }
        // === END OF ADDED SECTION ===

        // Connection established, set the main read/write callbacks
        bufferevent_setcb(bev, workerReadCallback, NULL, workerEventCallback, ctx);
        bufferevent_enable(bev, EV_READ | EV_WRITE);
    }
}

// Client implementation
class TaskClient
{
public:
    TaskClient(const std::string &host, int port)
        : host_(host), port_(port), base_(nullptr), bev_(nullptr) {}

    ~TaskClient()
    {
        // bufferevent_free also frees the associated socket
        if (bev_)
            bufferevent_free(bev_);
        if (base_)
            event_base_free(base_);
        // Clean up Protocol Buffers library
        google::protobuf::ShutdownProtobufLibrary();
    }

    bool connect()
    {
        // Create event base
        base_ = event_base_new();
        if (!base_)
        {
            std::cerr << "Could not create event base" << std::endl;
            return false;
        }

        // Create bufferevent for connection
        bev_ = bufferevent_socket_new(base_, -1, BEV_OPT_CLOSE_ON_FREE);
        if (!bev_)
        {
            std::cerr << "Could not create bufferevent" << std::endl;
            event_base_free(base_); // Clean up base if bufferevent creation fails
            base_ = nullptr;
            return false;
        }

        // Connect to server
        struct sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(port_);
        if (evutil_inet_pton(AF_INET, host_.c_str(), &sin.sin_addr) <= 0)
        {
            std::cerr << "Invalid address: " << host_ << std::endl;
            bufferevent_free(bev_);
            bev_ = nullptr;
            event_base_free(base_);
            base_ = nullptr;
            return false;
        }

        // Set initial callbacks for connection event
        bufferevent_setcb(bev_, NULL, NULL, clientEventCallback, this);

        if (bufferevent_socket_connect(bev_, (struct sockaddr *)&sin, sizeof(sin)) < 0)
        {
            std::cerr << "Error connecting to server at " << host_ << ":" << port_ << std::endl;
            bufferevent_free(bev_); // Clean up bufferevent on connection failure
            bev_ = nullptr;
            event_base_free(base_); // Clean up base on connection failure
            base_ = nullptr;
            return false;
        }

        // Enable read/write events after initiating connect.
        // The BEV_EVENT_CONNECTED will be reported when connection is established.
        bufferevent_enable(bev_, EV_READ | EV_WRITE);

        // Wait for the connection to be established
        // This is a simple blocking wait for the client example.
        // A real-world client might use a non-blocking approach.
        // We wait for the BEV_EVENT_CONNECTED event in the callback to signal success.
        std::cout << "Attempting to connect to " << host_ << ":" << port_ << std::endl;
        event_base_dispatch(base_); // Run the loop until event_base_loopexit is called in callback

        std::cout << "Connected to broker server." << std::endl;

        return true;
    }

    void submitTask(const std::string &script, const std::string &args, const std::string &taskId)
    {
        if (!bev_ || !base_)
        {
            std::cerr << "Client not connected." << std::endl;
            return;
        }

        // Create task request
        task::TaskRequest request;
        request.set_script(script);
        request.set_args(args);
        request.set_task_id(taskId);
        // client_id is not needed for client-to-broker request, broker will set it

        // Serialize the request
        std::string requestStr;
        if (!request.SerializeToString(&requestStr))
        {
            std::cerr << "Failed to serialize request" << std::endl;
            return;
        }

        // Send the size first
        uint32_t requestSize = htonl(requestStr.size());
        if (bufferevent_write(bev_, &requestSize, sizeof(requestSize)) < 0)
        {
            std::cerr << "Failed to write request size to server." << std::endl;
            // Consider re-connecting or handling the error appropriately
            return;
        }

        // Send the request
        if (bufferevent_write(bev_, requestStr.c_str(), requestStr.size()) < 0)
        {
            std::cerr << "Failed to write request data to server." << std::endl;
            // Consider re-connecting or handling the error appropriately
            return;
        }

        // Set callback to read response
        bufferevent_setcb(bev_, clientReadCallback, NULL, clientEventCallback, this);
        bufferevent_enable(bev_, EV_READ); // Ensure read is enabled for the response

        // Run the event loop to wait for the response
        event_base_dispatch(base_); // Loop until a response is received or error occurs
    }

private:
    static void clientReadCallback(struct bufferevent *bev, void *ctx)
    {
        struct evbuffer *input = bufferevent_get_input(bev);

        while (true)
        {
            // Read message size
            uint32_t msgSize;
            if (evbuffer_copyout(input, &msgSize, sizeof(msgSize)) < sizeof(msgSize))
            {
                return; // Not enough data yet for size
            }

            msgSize = ntohl(msgSize);

            // Check if we have the complete message
            if (evbuffer_get_length(input) < sizeof(msgSize) + msgSize)
            {
                return; // Wait for more data
            }

            // Remove the size field
            evbuffer_drain(input, sizeof(msgSize));

            // Read the protobuf message
            std::unique_ptr<char[]> data(new char[msgSize]);
            evbuffer_remove(input, data.get(), msgSize);

            // Parse the task response
            task::TaskResponse response;
            if (response.ParseFromArray(data.get(), msgSize))
            {
                std::cout << "Task " << response.task_id() << " completed: " << response.result() << std::endl;
            }
            else
            {
                std::cerr << "Failed to parse response" << std::endl;
            }

            // Exit the event loop after receiving a response
            event_base_loopexit(bufferevent_get_base(bev), NULL);
            return; // Process only one response per submission in this simple client
        }
    }

    static void clientEventCallback(struct bufferevent *bev, short events, void *ctx)
    {
        TaskClient *client = static_cast<TaskClient *>(ctx);

        if (events & BEV_EVENT_ERROR)
        {
            int err = EVUTIL_SOCKET_ERROR();
            std::cerr << "Error from bufferevent: " << evutil_socket_error_to_string(err) << std::endl;
        }
        if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT))
        {
            if (events & BEV_EVENT_EOF)
            {
                std::cerr << "Connection closed by server." << std::endl;
            }
            else if (events & BEV_EVENT_TIMEOUT)
            {
                std::cerr << "Connection timed out." << std::endl;
            }
            // Mark bufferevent and base as null before exiting loop, as they will be freed
            client->bev_ = nullptr;
            client->base_ = nullptr;
            event_base_loopexit(bufferevent_get_base(bev), NULL); // Exit the client's event loop
        }
        if (events & BEV_EVENT_CONNECTED)
        {
            std::cout << "Client connected successfully." << std::endl;
            // Connection established, the connect attempt loop can exit
            event_base_loopexit(bufferevent_get_base(bev), NULL);
        }
    }

    std::string host_;
    int port_;
    struct event_base *base_;
    struct bufferevent *bev_;
};

// Run the broker server
void runBroker(int clientPort, int workerPort)
{
    BrokerContext ctx;
    struct evconnlistener *clientListener;
    struct evconnlistener *workerListener;
    struct sockaddr_in clientSin, workerSin;

    // Initialize event base
    ctx.base = event_base_new();
    if (!ctx.base)
    {
        std::cerr << "Could not create event base" << std::endl;
        return;
    }

    // Set up the address for client connections
    memset(&clientSin, 0, sizeof(clientSin));
    clientSin.sin_family = AF_INET;
    clientSin.sin_addr.s_addr = htonl(INADDR_ANY);
    clientSin.sin_port = htons(clientPort);

    // Create the client listener
    clientListener = evconnlistener_new_bind(
        ctx.base, brokerAcceptCallback, &ctx,
        LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE, -1,
        (struct sockaddr *)&clientSin, sizeof(clientSin));

    if (!clientListener)
    {
        std::cerr << "Could not create client listener on port " << clientPort << std::endl;
        event_base_free(ctx.base);
        return;
    }

    // Set up the address for worker connections
    memset(&workerSin, 0, sizeof(workerSin));
    workerSin.sin_family = AF_INET;
    workerSin.sin_addr.s_addr = htonl(INADDR_ANY);
    workerSin.sin_port = htons(workerPort);

    // Create the worker listener
    workerListener = evconnlistener_new_bind(
        ctx.base, workerAcceptCallback, &ctx,
        LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE, -1,
        (struct sockaddr *)&workerSin, sizeof(workerSin));

    if (!workerListener)
    {
        std::cerr << "Could not create worker listener on port " << workerPort << std::endl;
        evconnlistener_free(clientListener);
        event_base_free(ctx.base);
        return;
    }

    // Start the dispatcher thread
    std::thread dispatcher(dispatcherThread, &ctx);
    dispatcher.detach(); // Detach the thread to run independently

    std::cout << "Broker server running on port " << clientPort << " for clients and port "
              << workerPort << " for workers" << std::endl;

    // Run the event loop
    event_base_dispatch(ctx.base);

    std::cout << "Broker server shutting down." << std::endl;

    // Clean up
    evconnlistener_free(clientListener);
    evconnlistener_free(workerListener);
    event_base_free(ctx.base);

    // Note: The dispatcher thread is detached, it might still be running.
    // For a clean shutdown, you might need a mechanism to signal the thread to exit.
}

// Run a worker node
void runWorker(const std::string &brokerId, int brokerPort, const std::string &workerId)
{
    WorkerContext ctx;
    ctx.worker_id = workerId;

    // Initialize event base
    ctx.base = event_base_new();
    if (!ctx.base)
    {
        std::cerr << "Could not create event base" << std::endl;
        return;
    }

    // Create bufferevent for connection to broker
    ctx.broker_bev = bufferevent_socket_new(ctx.base, -1, BEV_OPT_CLOSE_ON_FREE);
    if (!ctx.broker_bev)
    {
        std::cerr << "Could not create bufferevent" << std::endl;
        event_base_free(ctx.base);
        return;
    }

    // Connect to broker
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(brokerPort);
    if (evutil_inet_pton(AF_INET, brokerId.c_str(), &sin.sin_addr) <= 0)
    {
        std::cerr << "Invalid broker address: " << brokerId << std::endl;
        bufferevent_free(ctx.broker_bev);
        event_base_free(ctx.base);
        return;
    }

    // Set initial callbacks for connection event
    bufferevent_setcb(ctx.broker_bev, NULL, NULL, workerEventCallback, &ctx);

    if (bufferevent_socket_connect(ctx.broker_bev, (struct sockaddr *)&sin, sizeof(sin)) < 0)
    {
        std::cerr << "Error connecting to broker at " << brokerId << ":" << brokerPort << std::endl;
        bufferevent_free(ctx.broker_bev);
        event_base_free(ctx.base);
        return;
    }

    // Enable read/write events after initiating connect.
    // The BEV_EVENT_CONNECTED will be reported when connection is established.
    bufferevent_enable(ctx.broker_bev, EV_READ | EV_WRITE);

    // Wait for the connection to be established
    // We wait for the BEV_EVENT_CONNECTED event in the callback to signal success.
    std::cout << "Worker " << workerId << " attempting to connect to broker at " << brokerId << ":" << brokerPort << std::endl;
    event_base_dispatch(ctx.base); // Loop until event_base_loopexit is called in callback

    std::cout << "Worker " << workerId << " connected successfully to broker." << std::endl;

    // Register with broker
    task::WorkerRegistration reg;
    reg.set_worker_id(workerId);

    std::string regStr;
    if (!reg.SerializeToString(&regStr))
    {
        std::cerr << "Failed to serialize worker registration" << std::endl;
        // If serialization fails, the connection is likely still open but unusable for registration
        // Clean up and exit worker.
        bufferevent_free(ctx.broker_bev);
        ctx.broker_bev = nullptr;
        event_base_free(ctx.base);
        ctx.base = nullptr;
        return;
    }

    // Send the size first
    uint32_t regSize = htonl(regStr.size());
    if (bufferevent_write(ctx.broker_bev, &regSize, sizeof(regSize)) < 0)
    {
        std::cerr << "Failed to write registration size to broker." << std::endl;
        // Clean up and exit worker on write failure
        bufferevent_free(ctx.broker_bev);
        ctx.broker_bev = nullptr;
        event_base_free(ctx.base);
        ctx.base = nullptr;
        return;
    }

    // Send the registration
    if (bufferevent_write(ctx.broker_bev, regStr.c_str(), regStr.size()) < 0)
    {
        std::cerr << "Failed to write registration data to broker." << std::endl;
        // Clean up and exit worker on write failure
        bufferevent_free(ctx.broker_bev);
        ctx.broker_bev = nullptr;
        event_base_free(ctx.base);
        ctx.base = nullptr;
        return;
    }

    // The workerReadCallback is already set and enabled after successful connection.
    // It will handle the registration acknowledgment and subsequent task requests.

    // Run the event loop to process messages from the broker
    event_base_dispatch(ctx.base);

    std::cout << "Worker " << workerId << " shutting down." << std::endl;

    // Clean up is handled by the event loop exiting and BEV_OPT_CLOSE_ON_FREE
    // If base_ and broker_bev_ are not null, free them
    if (ctx.broker_bev)
        bufferevent_free(ctx.broker_bev);
    if (ctx.base)
        event_base_free(ctx.base);
}

// Example usage
int main(int argc, char **argv)
{
    // Initialize Protocol Buffers
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " [broker CLIENT_PORT WORKER_PORT | worker MASTER_HOST MASTER_PORT WORKER_ID | client HOST PORT]" << std::endl;
        return 1;
    }

    std::string mode = argv[1];

    if (mode == "broker")
    {
        if (argc != 4)
        {
            std::cerr << "Usage: " << argv[0] << " broker CLIENT_PORT WORKER_PORT" << std::endl;
            google::protobuf::ShutdownProtobufLibrary();
            return 1;
        }
        int clientPort = std::stoi(argv[2]);
        int workerPort = std::stoi(argv[3]);
        runBroker(clientPort, workerPort);
    }
    else if (mode == "worker")
    {
        if (argc != 5)
        {
            std::cerr << "Usage: " << argv[0] << " worker MASTER_HOST MASTER_PORT WORKER_ID" << std::endl;
            google::protobuf::ShutdownProtobufLibrary();
            return 1;
        }
        std::string brokerHost = argv[2];
        int brokerPort = std::stoi(argv[3]);
        std::string workerId = argv[4];
        runWorker(brokerHost, brokerPort, workerId);
    }
    else if (mode == "client")
    {
        if (argc != 4)
        {
            std::cerr << "Usage: " << argv[0] << " client HOST PORT" << std::endl;
            google::protobuf::ShutdownProtobufLibrary();
            return 1;
        }
        std::string host = argv[2];
        int port = std::stoi(argv[3]);

        TaskClient client(host, port);
        if (!client.connect())
        {
            google::protobuf::ShutdownProtobufLibrary();
            return 1;
        }

        std::string script = "function execute(args) return tostring(9+10) end";
        std::string args = "test";
        std::string taskId = "task0";

        client.submitTask(script, args, taskId);
    }
    else
    {
        std::cerr << "Usage: " << argv[0] << " [broker CLIENT_PORT WORKER_PORT | worker MASTER_HOST MASTER_PORT WORKER_ID | client HOST PORT]" << std::endl;
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }

    return 0;
}