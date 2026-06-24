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
import socket

import grpc
from os_brick.initiator import connector as brick_connector

from osbrick.gen import connector_pb2
from osbrick.gen import connector_pb2_grpc

LOG = logging.getLogger(__name__)

# Root helper for os-brick operations.  In a privileged container this
# is typically 'sudo' or '' (empty string if already root).
ROOT_HELPER = "sudo"


def _get_my_ip():
    """Return the IP address of the host.

    Uses the OSBRICK_MY_IP environment variable if set, otherwise
    resolves the hostname.  os-brick needs this for iSCSI initiator
    registration and connection tracking.
    """
    import os
    ip = os.environ.get("OSBRICK_MY_IP")
    if ip:
        return ip
    try:
        return socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        LOG.warning("Could not resolve hostname, falling back to 127.0.0.1")
        return "127.0.0.1"


class OsBrickConnectorServicer(connector_pb2_grpc.OsBrickConnectorServicer):
    """Maps gRPC calls to os-brick connector operations."""

    def GetConnectorProperties(self, request, context):
        """Return host initiator information from os-brick."""
        try:
            my_ip = _get_my_ip()
            props = brick_connector.get_connector_properties(
                ROOT_HELPER,
                my_ip,
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
        #
        # The typed fields (initiator, wwpns, host, multipath) are set
        # for convenience and logging.  The raw_json field carries the
        # complete dict as JSON, preserving original Python types
        # (bools, lists, ints) so the Go side can store it directly
        # in the node annotation and pass it to Cinder without type
        # coercion (e.g. Python True → string "True").
        extras = {}
        known_keys = {"initiator", "wwpns", "host", "multipath"}
        for key, value in props.items():
            if key not in known_keys:
                extras[key] = json.dumps(value) if not isinstance(value, str) else value

        return connector_pb2.ConnectorProperties(
            initiator=props.get("initiator", ""),
            wwpns=props.get("wwpns", []),
            host=props.get("host", ""),
            multipath=props.get("multipath", False),
            extras=extras,
            raw_json=json.dumps(props),
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
            device_info = conn.connect_volume(connection_info.get("data") or connection_info)
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
                connection_info.get("data") or connection_info,
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
            conn.extend_volume(connection_info.get("data") or connection_info)
        except Exception as e:
            LOG.exception("extend_volume failed")
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"extend_volume failed: {e}",
            )

        LOG.info("ExtendVolume: success")
        return connector_pb2.ExtendVolumeResponse()
