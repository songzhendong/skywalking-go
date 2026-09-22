# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.24 as builder

ENV CGO_ENABLED=0
WORKDIR /build
COPY test/e2e/base/dns-mock/main.go .
RUN go mod init dns-mock && go build -o dns-mock main.go

FROM alpine:3.11
COPY --from=builder /build/dns-mock /dns-mock
EXPOSE 53/udp
CMD ["/dns-mock"]
