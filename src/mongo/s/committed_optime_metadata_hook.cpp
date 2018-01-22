/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/s/committed_optime_metadata_hook.h"

#include "mongo/client/connection_string.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

namespace rpc {

namespace {
const char kLastCommittedOpTimeFieldName[] = "lastCommittedOpTime";
}

CommittedOpTimeMetadataHook::CommittedOpTimeMetadataHook(ServiceContext* service)
    : _service(service) {}

Status CommittedOpTimeMetadataHook::writeRequestMetadata(OperationContext* opCtx,
                                                         BSONObjBuilder* metadataBob) {
    return Status::OK();
}

Status CommittedOpTimeMetadataHook::readReplyMetadata(OperationContext* opCtx,
                                                      StringData replySource,
                                                      const BSONObj& metadataObj) {
    auto lastCommittedOpTimeField = metadataObj[kLastCommittedOpTimeFieldName];
    if (!lastCommittedOpTimeField.eoo()) {
        invariant(lastCommittedOpTimeField.type() == BSONType::bsonTimestamp);

        auto shardRegistry = Grid::get(_service)->shardRegistry();
        if (shardRegistry) {
            auto shard = [&] {
                try {
                    auto sourceHostAndPort = HostAndPort(replySource);
                    return Grid::get(_service)->shardRegistry()->getShardForHostNoReload(
                        sourceHostAndPort);
                } catch (const ExceptionFor<ErrorCodes::FailedToParse>& ex) {
                    // DBClientReplicaSet sends the replySource as a connection string, so we may
                    // fail to parse it as a HostAndPort.
                    auto connString =
                        uassertStatusOK(ConnectionString::parse(replySource.toString()));
                    invariant(connString.type() == ConnectionString::SET);
                    return Grid::get(_service)->shardRegistry()->getShardNoReload(
                        connString.getSetName());
                }
            }();

            if (shard) {
                shard->updateLastCommittedOpTime(LogicalTime(lastCommittedOpTimeField.timestamp()));
            }
        }
    }

    return Status::OK();
}

}  // namespace rpc
}  // namespace mongo
