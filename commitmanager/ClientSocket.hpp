/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <commitmanager/ErrorCode.hpp>
#include <commitmanager/MessageTypes.hpp>
#include <commitmanager/SnapshotDescriptor.hpp>

#include <crossbow/infinio/RpcClient.hpp>
#include <crossbow/string.hpp>

#include <cstdint>
#include <system_error>
#include <vector>

namespace tell {
namespace commitmanager {

/**
 * @brief Response for a Start-Transaction request
 */
class StartResponse final
        : public crossbow::infinio::RpcResponseResult<StartResponse, std::unique_ptr<SnapshotDescriptor>> {
    using Base = crossbow::infinio::RpcResponseResult<StartResponse, std::unique_ptr<SnapshotDescriptor>>;

public:
    using Base::Base;

private:
    friend Base;

    static constexpr ResponseType MessageType = ResponseType::START;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

    void processResponse(crossbow::buffer_reader& message);
};

/**
 * @brief Response for a Commit-Transaction request
 */
class CommitResponse final : public crossbow::infinio::RpcResponseResult<CommitResponse, bool> {
    using Base = crossbow::infinio::RpcResponseResult<CommitResponse, bool>;

public:
    using Base::Base;

    static constexpr ResponseType MessageType = ResponseType::COMMIT;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

private:
    friend Base;

    void processResponse(crossbow::buffer_reader& message);
};

/**
* @brief Response for a Get-Nodes request
*/
class ClusterStateResponse final : public crossbow::infinio::RpcResponseResult<ClusterStateResponse, std::unique_ptr<ClusterMeta>> {
    using Base = crossbow::infinio::RpcResponseResult<ClusterStateResponse, std::unique_ptr<ClusterMeta>>;

public:
    using Base::Base;

    static constexpr ResponseType MessageType = ResponseType::CLUSTER_STATE;

    static const std::error_category& errorCategory() {
        return error::get_error_category();
    }

private:
    friend Base;

    void processResponse(crossbow::buffer_reader& message);
};

/**
 * @brief Handles communication with one CommitManager server
 *
 * Sends RPC requests and returns the pending response.
 */
class ClientSocket final : public crossbow::infinio::RpcClientSocket {
    using Base = crossbow::infinio::RpcClientSocket;

public:
    using Base::Base;

    void connect(const crossbow::infinio::Endpoint& host);

    void shutdown();

    std::shared_ptr<StartResponse> startTransaction(crossbow::infinio::Fiber& fiber, bool readonly);

    std::shared_ptr<CommitResponse> commitTransaction(crossbow::infinio::Fiber& fiber, uint64_t version);

    std::shared_ptr<ClusterStateResponse> registerNode(
        crossbow::infinio::Fiber& fiber,
        crossbow::string host,
        crossbow::string tag
    );

    std::shared_ptr<CommitResponse> unregisterNode(
        crossbow::infinio::Fiber& fiber,
        crossbow::string host
    );

    std::shared_ptr<CommitResponse> transferOwnership(
        crossbow::infinio::Fiber& fiber,
        crossbow::string fromHost,
        crossbow::string toHost,
        std::vector<Partition> ranges
    );
};

} // namespace commitmanager
} // namespace tell
