# Copyright 2025 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""gRPC servicer that wraps os-brick volume operations.

This module implements the OsBrickConnectorServicer interface defined
in the protobuf API (proto/osbrick/v1/connector.proto).  Each RPC
delegates to the corresponding os-brick function.
"""

import json
import logging

import grpc
from os_brick.initiator import connector as brick_connector

from osbrick.gen import connector_pb2
from osbrick.gen import connector_pb2_grpc

LOG = logging.getLogger(__name__)

# Root helper for os-brick operations.  In a privileged container this
# is typically 'sudo' or '' (empty string if already root).
ROOT_HELPER = "sudo"


class OsBrickConnectorServicer(connector_pb2_grpc.OsBrickConnectorServicer):
    """Maps gRPC calls to os-brick connector operations."""

    def GetConnectorProperties(self, request, context):
        """Return host initiator information from os-brick."""
        try:
            props = brick_connector.get_connector_properties(
                ROOT_HELPER,
                multipath=True,
                enforce_multipath=False,
            )
        except Exception as e:
            LOG.exception("get_connector_properties failed")
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"get_connector_properties failed: {e}",
            )

        LOG.info("Connector properties: %s", props)

        # Map the os-brick dict to the proto message.
        extras = {}
        known_keys = {"initiator", "wwpns", "host", "multipath"}
        for key, value in props.items():
            if key not in known_keys:
                extras[key] = str(value)

        return connector_pb2.ConnectorProperties(
            initiator=props.get("initiator", ""),
            wwpns=props.get("wwpns", []),
            host=props.get("host", ""),
            multipath=props.get("multipath", False),
            extras=extras,
        )

    def ConnectVolume(self, request, context):
        """Attach a volume using os-brick and return the device path."""
        try:
            connection_info = json.loads(request.connection_info)
        except json.JSONDecodeError as e:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Invalid connection_info JSON: {e}",
            )

        driver_volume_type = connection_info.get("driver_volume_type", "")
        if not driver_volume_type:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "connection_info missing driver_volume_type",
            )

        LOG.info(
            "ConnectVolume: driver_volume_type=%s", driver_volume_type
        )

        try:
            conn = brick_connector.InitiatorConnector.factory(
                driver_volume_type,
                ROOT_HELPER,
                use_multipath=connection_info.get("multipath", False),
            )
            device_info = conn.connect_volume(connection_info.get("data", {}))
        except Exception as e:
            LOG.exception("connect_volume failed")
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"connect_volume failed: {e}",
            )

        device_path = device_info.get("path", "")
        multipath_device = device_info.get("multipath_device", "")

        LOG.info(
            "ConnectVolume: device_path=%s multipath_device=%s",
            device_path,
            multipath_device,
        )

        return connector_pb2.ConnectVolumeResponse(
            device_path=device_path,
            multipath_device=multipath_device,
        )

    def DisconnectVolume(self, request, context):
        """Detach a previously connected volume using os-brick."""
        try:
            connection_info = json.loads(request.connection_info)
        except json.JSONDecodeError as e:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Invalid connection_info JSON: {e}",
            )

        driver_volume_type = connection_info.get("driver_volume_type", "")
        if not driver_volume_type:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "connection_info missing driver_volume_type",
            )

        LOG.info(
            "DisconnectVolume: driver_volume_type=%s", driver_volume_type
        )

        try:
            conn = brick_connector.InitiatorConnector.factory(
                driver_volume_type,
                ROOT_HELPER,
                use_multipath=connection_info.get("multipath", False),
            )
            conn.disconnect_volume(
                connection_info.get("data", {}),
                None,  # device_info — os-brick looks it up internally
            )
        except Exception as e:
            LOG.exception("disconnect_volume failed")
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"disconnect_volume failed: {e}",
            )

        LOG.info("DisconnectVolume: success")
        return connector_pb2.DisconnectVolumeResponse()

    def ExtendVolume(self, request, context):
        """Rescan/extend a connected volume using os-brick."""
        try:
            connection_info = json.loads(request.connection_info)
        except json.JSONDecodeError as e:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Invalid connection_info JSON: {e}",
            )

        driver_volume_type = connection_info.get("driver_volume_type", "")
        if not driver_volume_type:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "connection_info missing driver_volume_type",
            )

        LOG.info(
            "ExtendVolume: driver_volume_type=%s", driver_volume_type
        )

        try:
            conn = brick_connector.InitiatorConnector.factory(
                driver_volume_type,
                ROOT_HELPER,
                use_multipath=connection_info.get("multipath", False),
            )
            conn.extend_volume(connection_info.get("data", {}))
        except Exception as e:
            LOG.exception("extend_volume failed")
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"extend_volume failed: {e}",
            )

        LOG.info("ExtendVolume: success")
        return connector_pb2.ExtendVolumeResponse()
