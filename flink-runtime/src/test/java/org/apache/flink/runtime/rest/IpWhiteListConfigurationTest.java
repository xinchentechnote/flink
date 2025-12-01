package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** 白名单配置单元测试. */
public class IpWhiteListConfigurationTest {

    private IpWhiteListConfiguration ipWhiteListConfig;

    @Before
    public void setUp() {}

    @Test
    public void testEmptyConfiguration() {
        Configuration config = new Configuration();
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);
        assertFalse(ipWhiteListConfig.isEnable());
        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.1"));
        assertTrue(ipWhiteListConfig.isAllowed("10.0.0.1"));
    }

    @Test
    public void testExactIpMatch() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "192.168.1.1,10.0.0.1");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);
        assertTrue(ipWhiteListConfig.isEnable());

        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.1"));
        assertTrue(ipWhiteListConfig.isAllowed("10.0.0.1"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.1.2"));
        assertFalse(ipWhiteListConfig.isAllowed("10.0.0.2"));
    }

    @Test
    public void testWildcardMatch() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "192.168.*.*,10.*.*.*");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.1"));
        assertTrue(ipWhiteListConfig.isAllowed("192.168.100.200"));
        assertFalse(ipWhiteListConfig.isAllowed("192.169.1.1"));

        assertTrue(ipWhiteListConfig.isAllowed("10.0.0.1"));
        assertTrue(ipWhiteListConfig.isAllowed("10.255.255.255"));
        assertFalse(ipWhiteListConfig.isAllowed("11.0.0.1"));
    }

    @Test
    public void testCidrMatch() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "192.168.1.0/24,10.0.0.0/8");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.1"));
        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.255"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.2.1"));

        assertTrue(ipWhiteListConfig.isAllowed("10.0.0.1"));
        assertTrue(ipWhiteListConfig.isAllowed("10.255.255.255"));
        assertFalse(ipWhiteListConfig.isAllowed("11.0.0.1"));
    }

    @Test
    public void testMixedRules() {
        Configuration config = new Configuration();
        config.setString(
                IpWhiteListConfiguration.IP_WHITE_LIST_KEY,
                "192.168.1.100,192.168.2.*,192.168.3.0/24");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.100"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.1.101"));

        assertTrue(ipWhiteListConfig.isAllowed("192.168.2.1"));
        assertTrue(ipWhiteListConfig.isAllowed("192.168.2.255"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.4.1"));

        assertTrue(ipWhiteListConfig.isAllowed("192.168.3.1"));
        assertTrue(ipWhiteListConfig.isAllowed("192.168.3.254"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.4.1"));
    }

    @Test
    public void testInvalidIpAddress() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "192.168.1.1");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertFalse(ipWhiteListConfig.isAllowed("invalid-ip"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.1.256"));
        assertFalse(ipWhiteListConfig.isAllowed("192.168.1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCidrRule() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "192.168.1.1/33");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);
    }

    @Test
    public void testEdgeCases() {
        Configuration config = new Configuration();
        config.setString(
                IpWhiteListConfiguration.IP_WHITE_LIST_KEY, "0.0.0.0,255.255.255.255,127.0.0.1");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertTrue(ipWhiteListConfig.isAllowed("0.0.0.0"));
        assertTrue(ipWhiteListConfig.isAllowed("255.255.255.255"));
        assertTrue(ipWhiteListConfig.isAllowed("127.0.0.1"));
    }

    @Test
    public void testWhitespaceHandling() {
        Configuration config = new Configuration();
        config.setString(IpWhiteListConfiguration.IP_WHITE_LIST_KEY, " 192.168.1.1 , 10.0.0.1 ");
        ipWhiteListConfig = IpWhiteListConfiguration.from(config);

        assertTrue(ipWhiteListConfig.isAllowed("192.168.1.1"));
        assertTrue(ipWhiteListConfig.isAllowed("10.0.0.1"));
    }
}
