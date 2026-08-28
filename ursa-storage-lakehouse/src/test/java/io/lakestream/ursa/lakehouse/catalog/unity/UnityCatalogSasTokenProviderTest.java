/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class UnityCatalogSasTokenProviderTest {

    UnityCatalogSasTokenProvider provider = new UnityCatalogSasTokenProvider();

    private static final String TOKEN1 = "token1";

    private static final String TOKEN2 = "token2";

    @AfterEach
    public void clear() {
        UnityCatalogSasTokenProvider.clearTokenMap();
    }

    @Test
    public void testGetSASToken_ExactThreeLevelMatch() throws Exception {
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/line-item-config", TOKEN1);
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/line-item-config-non-loyalty", TOKEN2);

        String result1 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config/2bbc6b16-95cc-4dc1-9c00-2aca72ca40f6.parquet", "read");
        assertEquals(TOKEN1, result1);

        result1 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config/2bbc6b16-95cc-4dc1-9c00-2aca72ca40f6.parquet", "write");
        assertEquals(TOKEN1, result1);

        String result2 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config-non-loyalty/subfolder/data.parquet", "read");
        assertEquals(TOKEN2, result2);

        result2 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config-non-loyalty/subfolder/data.parquet", "write");
        assertEquals(TOKEN2, result2);
    }

    @Test
    public void testGetSASToken_DeepNestedPaths() throws Exception {
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/line-item-config", TOKEN1);
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/line-item-config-non-loyalty", TOKEN2);

        String result1 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config/deep/nested/path/multiple/levels/file.parquet", "read");
        assertEquals(TOKEN1, result1);

        String result2 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/line-item-config-non-loyalty/2024/10/15/hour=12/data.parquet", "write");
        assertEquals(TOKEN2, result2);
    }

    @Test
    public void testGetSASToken_NoMatchingContainer() throws Exception {
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/line-item-config", TOKEN1);
        try {
            provider.getSASToken("test-account", "test-fs",
                "/other/container/path/file.parquet", "read");
            fail();
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e);
        }
        try {
            provider.getSASToken("test-account", "test-fs",
                "/different/prefix/line-item-config/file.parquet", "read");
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e);
        }

        try {
            provider.getSASToken("test-account", "test-fs",
                "/swan/raw", "read");
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e);
        }

        try {
            provider.getSASToken("test-account", "test-fs", "", "read");
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e);
        }
    }

    @Test
    public void testGetSASToken_PriorityWithSimilarPaths() throws Exception {
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/a", "token-a");
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/abc", "token-abc");
        UnityCatalogSasTokenProvider.updateToken("/swan/raw/ab", "token-ab");

        String result1 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/a/file.parquet", "read");
        assertEquals("token-a", result1);

        String result2 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/ab/file.parquet", "read");
        assertEquals("token-ab", result2);

        String result3 = provider.getSASToken("test-account", "test-fs",
            "/swan/raw/abc/file.parquet", "read");
        assertEquals("token-abc", result3);
    }
}
