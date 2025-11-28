package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/** 白名单配置类. */
public class IpWhiteListConfiguration {

    private final List<IpMatcher> matchers = new ArrayList<>();

    public static final ConfigOption<String> IP_WHITE_LIST_KEY =
            ConfigOptions.key("web-or-rest.ip-whitelist")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("default empty means disable ip white list.");

    public IpWhiteListConfiguration(Configuration configuration) {
        Optional<String> ipWhiteList = configuration.getOptional(IP_WHITE_LIST_KEY);
        ipWhiteList.ifPresent(
                ips -> {
                    for (String rule : ips.split(",")) {
                        rule = rule.trim();
                        matchers.add(parseRule(rule));
                    }
                });
    }

    public boolean isEnable() {
        // 配置为空时，不启用ip白名单，允许所有ip访问
        return !matchers.isEmpty();
    }

    public boolean isAllowed(String ip) {
        if (matchers.isEmpty()) {
            // 配置为空时，不启用ip白名单，允许所有ip访问
            return true;
        }
        for (IpMatcher matcher : matchers) {
            if (matcher.match(ip)) {
                return true;
            }
        }
        return false;
    }

    private IpMatcher parseRule(String rule) {
        if (rule.contains("/")) {
            return new CidrMatcher(rule);
        } else if (rule.contains("*")) {
            return new WildcardMatcher(rule);
        } else {
            return new IpMatcher() {
                @Override
                public boolean match(String ip) {
                    return rule.equals(ip);
                }
            };
        }
    }

    interface IpMatcher {
        boolean match(String ip);
    }

    static class CidrMatcher implements IpMatcher {
        private final int maskBits;
        private final byte[] networkBytes;

        public CidrMatcher(String cidr) {
            try {
                String[] parts = cidr.split("/");
                maskBits = Integer.parseInt(parts[1]);
                if (maskBits > 32 || maskBits < 0) {
                    throw new IllegalArgumentException("Invalid CIDR rule: " + cidr);
                }
                networkBytes = InetAddress.getByName(parts[0]).getAddress();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid CIDR rule: " + cidr);
            }
        }

        @Override
        public boolean match(String ip) {
            try {
                byte[] ipBytes = InetAddress.getByName(ip).getAddress();

                int fullBytes = maskBits / 8;
                int remainingBits = maskBits % 8;

                for (int i = 0; i < fullBytes; i++) {
                    if (ipBytes[i] != networkBytes[i]) {
                        return false;
                    }
                }

                if (remainingBits > 0) {
                    int mask = ~((1 << (8 - remainingBits)) - 1);
                    if ((ipBytes[fullBytes] & mask) != (networkBytes[fullBytes] & mask)) {
                        return false;
                    }
                }

                return true;

            } catch (UnknownHostException e) {
                return false;
            }
        }
    }

    static class WildcardMatcher implements IpMatcher {
        private final Pattern pattern;

        public WildcardMatcher(String wildcard) {
            // 192.168.*.* -> 192\.168\.\d+\.\d+
            String regex = wildcard.replace(".", "\\.").replace("*", "\\d+");
            this.pattern = Pattern.compile("^" + regex + "$");
        }

        @Override
        public boolean match(String ip) {
            return pattern.matcher(ip).matches();
        }
    }
}
