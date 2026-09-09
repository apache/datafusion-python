# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Config for the Jupyter kernel that executes the docs pages.

``build.sh`` copies this into a throwaway ``IPYTHONDIR`` profile, which is
where ``IPKernelApp`` looks for it. It is not on the Jupyter config path:
``IPKernelApp`` derives from ``BaseIPythonApplication`` and reads the IPython
profile directory, so ``JUPYTER_CONFIG_PATH`` has no effect on it.
"""

import logging

# ipykernel 7 warns at every kernel start that the ZeroMQ channels are
# unencrypted TCP. With `nb_execution_mode = "force"` the docs build starts a
# kernel per executed page, so the message repeats a dozen times per build and
# buries the output that matters.
#
# It does not describe a risk this build has. The kernel is a short-lived child
# process on the same machine, executing pages from this repository, reachable
# only over loopback. There is no second party to the conversation.
#
# Neither remedy the message itself suggests is usable here:
#
# - `transport="ipc"` does silence it, and it did work in isolation, but the
#   real build then fails with "Kernel didn't respond in 60 seconds".
# - `KernelManager.transport_encryption = "auto"` fails at kernel start on
#   jupyter_client 8.9.1, which hands the provisioned CurveZMQ key to the
#   client as `str` where a `bytes` trait is expected.
#
# So the message is filtered rather than the condition removed. A filter is
# used in preference to `c.Application.log_level = "ERROR"` so that any *other*
# kernel warning still reaches the build log. If ipykernel rewords the message
# this filter stops matching and the warning comes back, which is the right way
# for it to fail.
_SUPPRESSED = "Kernel is running over TCP without encryption"


class _DropTransportEncryptionNotice(logging.Filter):
    """Drop ipykernel's unencrypted-TCP notice, and nothing else."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False only for the one message this build does not need."""
        return _SUPPRESSED not in record.getMessage()


# Traitlets names an Application's logger after its class.
logging.getLogger("IPKernelApp").addFilter(_DropTransportEncryptionNotice())
