/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/** 白名单处理类. */
public class IpWhiteListHandler extends SimpleChannelInboundHandler<HttpObject> {

    private IpWhiteListConfiguration configuration;

    private static final AttributeKey<Boolean> IP_VERIFIED_KEY = AttributeKey.valueOf("ipVerified");

    public IpWhiteListHandler(IpWhiteListConfiguration configuration) {
        this.configuration = configuration;
    }

    private boolean isAllowedIp(ChannelHandlerContext ctx) {
        Boolean ipVerified = ctx.channel().attr(IP_VERIFIED_KEY).get();
        if (null != ipVerified && ipVerified) {
            return true;
        }
        InetSocketAddress remoteAddr = (InetSocketAddress) ctx.channel().remoteAddress();
        return configuration.isAllowed(remoteAddr.getAddress().getHostAddress());
    }

    private boolean isAllowedIp(ChannelHandlerContext ctx, HttpRequest request) {
        Boolean ipVerified = ctx.channel().attr(IP_VERIFIED_KEY).get();
        if (null != ipVerified && ipVerified) {
            return true;
        }
        InetSocketAddress remoteAddr = (InetSocketAddress) ctx.channel().remoteAddress();
        return configuration.isAllowed(remoteAddr.getAddress().getHostAddress());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (configuration.isEnable()) {
            if (!isAllowedIp(ctx)) {
                // response 403 to client
                InetAddress addr = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
                sendResponseAndClose(
                        ctx,
                        "Ip [" + addr.getHostAddress() + "] not allowed.",
                        HttpResponseStatus.FORBIDDEN);
                return;
            }
        }
        ctx.channel().attr(IP_VERIFIED_KEY).set(true);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (configuration.isEnable()) {
            Boolean ipVerified = ctx.channel().attr(IP_VERIFIED_KEY).get();
            if (null == ipVerified || !ipVerified) {
                ReferenceCountUtil.release(msg);
                return;
            }
            if (msg instanceof HttpRequest) {
                if (!isAllowedIp(ctx, (HttpRequest) msg)) {
                    InetAddress addr = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
                    // response 403 to client
                    sendResponseAndClose(
                            ctx,
                            "Ip [" + addr.getHostAddress() + "] not allowed.",
                            HttpResponseStatus.FORBIDDEN);
                    return;
                }
            }
            // 传递消息到下一个处理器
            ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
        }
    }

    private void sendResponseAndClose(
            ChannelHandlerContext ctx, String content, HttpResponseStatus status) {
        FullHttpResponse response = buildResponse(content, status);
        ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private FullHttpResponse buildResponse(String content, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(content, StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        return response;
    }
}
