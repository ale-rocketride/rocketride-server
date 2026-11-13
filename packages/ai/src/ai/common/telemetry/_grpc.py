# MIT License
#
# Copyright (c) 2026 Aparavi Software AG
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
The gRPC client instrumentation, with the tracer provider it is given.

The contrib GrpcInstrumentorClient reads the provider from each channel call's
arguments, where it never is, so its spans go to the global provider. A run's
process sets none (a node, such as crewai, may set its own), and those spans
would be lost. Imported only by _deps.instrument(), with the contrib package.

This class has its own instance, apart from the contrib one: a node that also
instruments gRPC with GrpcInstrumentorClient adds a second interceptor. Only the
outermost client span is measured, so each call still counts once; the cost is
one more span per call. wrapper_fn uses contrib internals (_filter, the hooks,
intercept_channel); the pinned release and the test that a call's span reaches
the given provider catch a contrib change.
"""

from typing import Any

from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient, client_interceptor
from opentelemetry.instrumentation.grpc.grpcext import intercept_channel


class GrpcClient(GrpcInstrumentorClient):
    """GrpcInstrumentorClient whose channels use the provider given to instrument()."""

    def _instrument(self, **kwargs: Any) -> None:
        self._rocketride_provider = kwargs.get('tracer_provider')
        super()._instrument(**kwargs)

    def wrapper_fn(self, original_func: Any, instance: Any, args: Any, kwargs: Any) -> Any:
        channel = original_func(*args, **kwargs)
        return intercept_channel(
            channel,
            client_interceptor(
                tracer_provider=self._rocketride_provider,
                filter_=self._filter,
                request_hook=self._request_hook,
                response_hook=self._response_hook,
            ),
        )
