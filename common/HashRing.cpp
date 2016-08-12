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
 *     Nikolas Göbel <ngoebel@student.ethz.ch>
 */
#include <commitmanager/HashRing.hpp>

// #include <functional>
#include <iterator>
#include <limits>


namespace tell {
namespace commitmanager {

    // Alternative hash function, used only for testing
    // const auto hashFn = std::hash<std::string>();

    crossbow::string HashRing::writeHash(Hash hash) {
        Hash tmp = hash;
        char buffer[128];
        char* d = std::end(buffer);
        
        do {
            --d;
            *d = "0123456789"[tmp % 10];
            tmp /= 10;
        } while (tmp != 0);
        
        int len = std::end(buffer) - d;
        return crossbow::string(d, len);
    }

    Hash HashRing::getPartitionToken(uint64_t tableId, uint64_t key) {
        Hash hash;
        crossbow::string composite_key = crossbow::to_string(tableId) + crossbow::to_string(key);
        MurmurHash3_x64_128(composite_key.data(), composite_key.size(), HashRing::SEED, &hash);
        // hash = hashFn(composite_key.data());
            
        return std::move(hash);
    }

    Hash HashRing::getPartitionToken(const crossbow::string& nodeName, uint32_t vnode) {
        Hash hash;
        crossbow::string token = crossbow::to_string(vnode) + nodeName;
        MurmurHash3_x64_128(token.data(), token.size(), HashRing::SEED, &hash);
        // hash = hashFn(token.data());

        return std::move(hash);
    }

    Hash HashRing::insertNode(const crossbow::string& nodeName) {
        PartitionMeta meta(nodeName);
        if (nodeRing.empty()) {
            // First node is definitely not bootstrapping
            meta.isBootstrapping = false;
        }

        Hash hash;
        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            hash = getPartitionToken(nodeName, vnode);

            // We have to remember the previous owner of this range.
            auto prevPartition = getNode(hash);
            if (prevPartition != nullptr) {
                meta.previousOwner = prevPartition->owner;
            }
            
            nodeRing.emplace(hash, meta);
        }
        return std::move(hash);
    }

    std::vector<Partition> HashRing::removeNode(const crossbow::string& nodeName) {
        std::vector<Partition> transfers;

        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            Hash hash = getPartitionToken(nodeName, vnode);

            auto search = nodeRing.find(hash);
            if (search == nodeRing.end()) {
                LOG_ERROR("Attempted to remove a node that is not in the ring.");
            } else {
                nodeRing.erase(search);

                if (nodeRing.empty()) {
                    LOG_WARN("No other nodes available. Removing this node will lead to complete loss of data.");
                    return transfers;
                }   

                // Find the ranges the node used to own
                auto ranges = getRanges(nodeName);
                for (const auto& range : ranges) {
                    auto newPartition = getNode(range.end);
                    if (newPartition != nullptr) {
                        transfers.emplace_back(newPartition->owner, range.start, range.end);
                    }
                }
            }
        }

        return std::move(transfers);
    }

    void HashRing::transferOwnership(Hash rangeEnd) {
        auto search = nodeRing.find(rangeEnd);
        if (search == nodeRing.end()) {
            LOG_ERROR("Not a managed partition. Can't transfer ownership.");
        } else {
            search->second.isBootstrapping = false;
        }
    }

    const PartitionMeta* HashRing::getNode(uint64_t tableId, uint64_t key) const {
        Hash token = getPartitionToken(tableId, key);
        return getNode(token);
    }

    const PartitionMeta* HashRing::getNode(Hash token) const {
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

    // Deprecated
    const std::map<Hash, PartitionMeta>& HashRing::getRing() const {
        return nodeRing;
    }

    void HashRing::getRange(std::vector<Partition>& ranges, Hash hash) const {
        auto lowerBound = nodeRing.lower_bound(hash);

        // @TODO These cases can be simplified into the ones presented
        // in the thesis.

        if (hash <= nodeRing.begin()->first) {
            // Case 0: first node encountered in clockwise direction
            LOG_DEBUG("Case 0");
            ranges.emplace_back(nodeRing.begin()->second.owner, nodeRing.rbegin()->first + 1, std::numeric_limits<Hash>::max()-1);
        }
        
        if (lowerBound == nodeRing.end()) {
            // Case 1: lowerBound wraps around
            LOG_DEBUG("Case 1");
            auto neighbour = std::prev(lowerBound);
            crossbow::string owner = nodeRing.begin()->second.owner;

            ranges.emplace_back(owner, neighbour->first + 1, hash);
        } 
        
        if (lowerBound == nodeRing.begin()) {
            // Case 2: prev(lowerBound) wraps around
            LOG_DEBUG("Case 2");
            crossbow::string owner = nodeRing.begin()->second.owner;
            ranges.emplace_back(owner, (Hash) 0, hash);
        }

        if (lowerBound != nodeRing.begin() && lowerBound != nodeRing.end()) {
            // Case 3: none wrap around
            LOG_DEBUG("Case 3");
            crossbow::string owner = lowerBound->second.owner;
            auto neighbour = std::prev(lowerBound);

            ranges.emplace_back(owner, neighbour->first + 1, hash);
        }    
    }

    std::vector<Partition> HashRing::getRanges(const crossbow::string& nodeName) const {
        std::vector<Partition> ranges;
        
        for (uint32_t vnode = 0; vnode < numVirtualNodes; vnode++) {
            Hash hash = getPartitionToken(nodeName, vnode);
            getRange(ranges, hash);
        }

        return std::move(ranges);
    }

    const bool HashRing::isActive(const crossbow::string& nodeName) const {
        Hash nodeToken = getPartitionToken(nodeName, 0);
        auto search = nodeRing.find(nodeToken);

        return search != nodeRing.end();
    }

    void HashRing::serialize(crossbow::buffer_writer& writer) const {
        writer.write<uint32_t>(numVirtualNodes);
        
        writer.write<uint32_t>(nodeRing.size());
        for (const auto& partitionIt : nodeRing) {
            writer.write<Hash>(partitionIt.first);

            writer.write<uint32_t>(partitionIt.second.owner.size());
            writer.write(partitionIt.second.owner.data(), partitionIt.second.owner.size());

            writer.write<uint32_t>(partitionIt.second.previousOwner.size());
            writer.write(partitionIt.second.previousOwner.data(), partitionIt.second.previousOwner.size());

            writer.write<bool>(partitionIt.second.isBootstrapping);
        }
    }

    std::unique_ptr<HashRing> HashRing::deserialize(crossbow::buffer_reader& reader) {
        auto numVirtualNodes = reader.read<uint32_t>();
        auto numPartitions = reader.read<uint32_t>();

        std::unique_ptr<HashRing> ring(new HashRing(numVirtualNodes));

        for (uint32_t i = 0; i < numPartitions; ++i) {
            Hash partitionId = reader.read<Hash>();

            uint32_t ownerSize = reader.read<uint32_t>();
            crossbow::string owner(reader.read(ownerSize), ownerSize);

            uint32_t prevOwnerSize = reader.read<uint32_t>();
            crossbow::string prevOwner(reader.read(prevOwnerSize), prevOwnerSize);

            bool isBootstrapping = reader.read<bool>();

            PartitionMeta meta(owner, prevOwner, isBootstrapping);
            ring->nodeRing.emplace(partitionId, meta); 
        }

        return ring;
    }

}
}