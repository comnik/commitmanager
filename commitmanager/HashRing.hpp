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
#include <iterator>
#include <limits>

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

            static crossbow::string writeHash(Hash hash);

            static bool isSubPartition(Hash parentStart, Hash parentEnd, Hash childStart, Hash childEnd); 
            
            static bool inPartition(Hash token, Hash rangeStart, Hash rangeEnd); 

            static Hash getPartitionToken(uint64_t tableId, uint64_t key);
            static Hash getPartitionToken(const crossbow::string& nodeName, uint32_t vnode);

            Hash insertNode(const crossbow::string& nodeName, const Node& node);
            
            std::vector<Partition> removeNode(const crossbow::string& nodeName);
            
            void clear();

            const Node* getNode(uint64_t tableId, uint64_t key);
            const Node* getNode(Hash token);
            const Node* getPreviousNode(Hash token);

            const std::map<Hash, Node> getRing() const;

            const bool isActive(const crossbow::string& nodeName) const;

            void getRange(std::vector<Partition>& ranges, Hash hash) const;
            std::vector<Partition> getRanges(const crossbow::string& nodeName) const;

            const bool isEmpty() { return nodeRing.empty(); }

        private:
            // Murmur seed
            static const uint32_t SEED = 0;

            const size_t numVirtualNodes;
            std::map<Hash, Node> nodeRing;
    };

    template <class Node>
    crossbow::string HashRing<Node>::writeHash(Hash hash) {
        Hash tmp = hash;
        char buffer[128];
        char* d = std::end( buffer );
        
        do {
            --d;
            *d = "0123456789"[tmp % 10];
            tmp /= 10;
        } while (tmp != 0);
        
        int len = std::end( buffer ) - d;
        return crossbow::string(d, len);
    }

    /**
     * Checks wether a partition is contained inside another.
     */
    template <class Node>
    bool HashRing<Node>::isSubPartition(Hash parentStart, Hash parentEnd, Hash childStart, Hash childEnd) {
        return (childStart >= parentStart && childEnd <= parentEnd);
    }

    /**
     * Checks wether a token is contained in the given range.
     */
    template <class Node>
    bool HashRing<Node>::inPartition(Hash token, Hash rangeStart, Hash rangeEnd) {
        return (token >= rangeStart && token <= rangeEnd);
    }

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
            nodeRing.emplace(hash, node);
        }
        return std::move(hash);
    }

    template <class Node>
    std::vector<Partition> HashRing<Node>::removeNode(const crossbow::string& nodeName) {
        std::vector<Partition> transfers;
        
        for (size_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            Hash hash = getPartitionToken(nodeName, vnode);

            auto search = nodeRing.find(hash);
            if (search == nodeRing.end()) {
                LOG_ERROR("Attempted to remove a node that is not in the ring.");
            } else {
                nodeRing.erase(search);

                // Find the ranges the node used to own
                auto ranges = getRanges(nodeName);
                for (const auto& range : ranges) {
                    auto newOwner = getNode(range.end);
                    transfers.emplace_back(*newOwner, range.start, range.end);
                }
            }
        }

        return std::move(transfers);
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
    const std::map<Hash, Node> HashRing<Node>::getRing() const {
        return nodeRing;
    }

    /**
     * Returns the node that used to own the given token.
     */
    template <class Node>
    const Node* HashRing<Node>::getPreviousNode(Hash token) {
        if (nodeRing.empty()) {
            return nullptr;
        } else {
            auto owner = nodeRing.lower_bound(token);
            if (owner == nodeRing.end()) {
                owner = nodeRing.begin();
            }

            // We have to use std::next even though we 
            // are interested in the 'previous' owner. This is because
            // the next highest node used to own the given token.
            auto prevOwner = std::next(owner);
            if (prevOwner == nodeRing.end()) {
                prevOwner = nodeRing.begin();
            }
            return &prevOwner->second;
        }
    }

    template <class Node>
    void HashRing<Node>::getRange(std::vector<Partition>& ranges, Hash hash) const {
        auto lowerBound = nodeRing.lower_bound(hash);

        if (hash <= nodeRing.begin()->first) {
            // Case 0: first node encountered in clockwise direction
            LOG_DEBUG("Case 0");
            ranges.emplace_back(nodeRing.begin()->second, nodeRing.rbegin()->first + 1, std::numeric_limits<Hash>::max()-1);
        }
        
        if (lowerBound == nodeRing.end()) {
            // Case 1: lowerBound wraps around
            LOG_DEBUG("Case 1");
            auto neighbour = std::prev(lowerBound);
            crossbow::string owner = nodeRing.begin()->second;

            ranges.emplace_back(owner, neighbour->first + 1, hash);
        } 
        
        if (lowerBound == nodeRing.begin()) {
            // Case 2: prev(lowerBound) wraps around
            LOG_DEBUG("Case 2");
            crossbow::string owner = nodeRing.begin()->second;
            ranges.emplace_back(owner, (Hash) 0, hash);
        }

        if (lowerBound != nodeRing.begin() && lowerBound != nodeRing.end()) {
            // Case 3: none wrap around
            LOG_DEBUG("Case 3");
            crossbow::string owner = lowerBound->second;
            auto neighbour = std::prev(lowerBound);

            ranges.emplace_back(owner, neighbour->first + 1, hash);
        }    
    }

    template <class Node>
    std::vector<Partition> HashRing<Node>::getRanges(const crossbow::string& nodeName) const {
        std::vector<Partition> ranges;
        
        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            Hash hash = getPartitionToken(nodeName, vnode);
            getRange(ranges, hash);
        }

        return std::move(ranges);
    }

    template <class Node>
    const bool HashRing<Node>::isActive(const crossbow::string& nodeName) const {
        Hash nodeToken = getPartitionToken(nodeName, 0);
        auto search = nodeRing.find(nodeToken);

        return search != nodeRing.end();
    }

} // namespace store
} // namespace tell
