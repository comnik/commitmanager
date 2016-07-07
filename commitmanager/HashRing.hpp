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

#include <map>

#include <crossbow/string.hpp>
#include <crossbow/logger.hpp>

#include <commitmanager/MessageTypes.hpp>

#include "MurmurHash3.h"

namespace tell {
namespace commitmanager {
    /**
     * @brief Implementation of consistent hashing.
     */
    template <class Node>
    class HashRing {
        public:
            HashRing(size_t numVirtualNodes) 
                : numVirtualNodes(numVirtualNodes) {}
            
            static Hash getPartitionToken(uint64_t tableId, uint64_t key);
            static Hash getPartitionToken(const crossbow::string& nodeName, uint32_t vnode);

            Hash insertNode(const crossbow::string& nodeName, const Node& node);
            
            void removeNode(const crossbow::string& nodeName);
            
            void clear();

            const Node* getNode(uint64_t tableId, uint64_t key);
            const Node* getNode(Hash token);

            const bool isActive(const crossbow::string& nodeName);

            std::vector<Partition> getRanges(const crossbow::string& nodeName);

        private:
            // Murmur seed
            static const uint32_t SEED = 0;

            const size_t numVirtualNodes;
            std::map<Hash, Node> nodeRing;
    };

    template <class Node>
    Hash HashRing<Node>::getPartitionToken(uint64_t tableId, uint64_t key) {
        Hash hash;
        crossbow::string composite_key = crossbow::to_string(tableId) + crossbow::to_string(key);
        MurmurHash3_x64_128(composite_key.data(), composite_key.size(), HashRing<Node>::SEED, &hash);
        
        return std::move(hash);
    }

    template <class Node>
    Hash HashRing<Node>::getPartitionToken(const crossbow::string& nodeName, uint32_t vnode) {
        Hash hash;
        crossbow::string token = crossbow::to_string(vnode) + nodeName;
        MurmurHash3_x64_128(token.data(), token.size(), HashRing<Node>::SEED, &hash);

        return std::move(hash);
    }

    template <class Node>
    Hash HashRing<Node>::insertNode(const crossbow::string& nodeName, const Node& node) {
        Hash hash;
        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            hash = getPartitionToken(nodeName, vnode);
            nodeRing[hash] = node;
        }
        return std::move(hash);
    }

    template <class Node>
    void HashRing<Node>::removeNode(const crossbow::string& nodeName) {
        Hash hash;
        for (size_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            hash = getPartitionToken(nodeName, vnode);
            nodeRing.erase(hash);
        }
    }

    template <class Node>
    void HashRing<Node>::clear() {
        nodeRing.clear();
    }

    template <class Node>
    const Node* HashRing<Node>::getNode(uint64_t tableId, uint64_t key) {
        Hash token = getPartitionToken(tableId, key);
        return getNode(token);
    }

    template <class Node>
    const Node* HashRing<Node>::getNode(Hash token) {
        if (nodeRing.empty()) {
            return nullptr;
        } else {
            auto it = nodeRing.lower_bound(token);
            if (it == nodeRing.end()) {
                it = nodeRing.begin();
            }
            return &it->second;
        }
    }

    template <class Node>
    std::vector<Partition> HashRing<Node>::getRanges(const crossbow::string& nodeName) {
        std::vector<Partition> ranges;
        
        Hash hash;
        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            hash = getPartitionToken(nodeName, vnode);

            // The new vnode is inserted between two others.
            // The one on the higher end of the range is the owner that
            // has to be contacted for a key transfer.

            auto rangeIterators = nodeRing.equal_range(hash);
            
            crossbow::string owner = rangeIterators.second->second;
            crossbow::string otherOwner = rangeIterators.first->second;
            
            Hash rangeStart = rangeIterators.first->first;
            Hash rangeEnd = rangeIterators.second->first;

            if (rangeStart > rangeEnd) {
                LOG_INFO("range_start > range_end");
                std::swap(rangeStart, rangeEnd);
                std::swap(owner, otherOwner);
            }

            ranges.emplace_back(owner, rangeStart, rangeEnd);
        }

        return std::move(ranges);
    }

    template <class Node>
    const bool HashRing<Node>::isActive(const crossbow::string& nodeName) {
        Hash nodeToken = getPartitionToken(nodeName, 0);
        auto search = nodeRing.find(nodeToken);

        return search != nodeRing.end();
    }

} // namespace store
} // namespace tell
