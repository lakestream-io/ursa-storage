/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.lakehouse.utils.TableNameFormatUtils;
import org.junit.jupiter.api.Test;

class S3IdentifierFormatterTest {

    @Test
    void testValidName() {
        assertEquals("mytable", TableNameFormatUtils.formatS3Identifier("mytable"));
    }

    @Test
    void testSlashReplacement() {
        assertEquals("my___table", TableNameFormatUtils.formatS3Identifier("my/table"));
    }

    @Test
    void testDotReplacement() {
        assertEquals("my_table", TableNameFormatUtils.formatS3Identifier("my.table"));
    }

    @Test
    void testDashReplacement() {
        assertEquals("my__table", TableNameFormatUtils.formatS3Identifier("my-table"));
    }

    @Test
    void testColonReplacement() {
        assertEquals("my____table", TableNameFormatUtils.formatS3Identifier("my:table"));
    }

    @Test
    void testUppercaseConversion() {
        // 'A' → 'a_' according to your rules
        assertEquals("a_b", TableNameFormatUtils.formatS3Identifier("Ab"));
        assertEquals("t_ablen_ame", TableNameFormatUtils.formatS3Identifier("TableName"));
    }

    @Test
    void testReservedAwsPrefix() {
        assertEquals("a_w_s_b_illing", TableNameFormatUtils.formatS3Identifier("AWSBilling"));
        assertEquals("x_awsb_illing", TableNameFormatUtils.formatS3Identifier("awsBilling"));
    }

    @Test
    void testRemoveLeadingTrailingUnderscores() {
        assertEquals("name", TableNameFormatUtils.formatS3Identifier("__name__"));
    }

    @Test
    void testEmptyInputBecomesDefault() {
        try {
            TableNameFormatUtils.formatS3Identifier("");
            fail("Expected IllegalArgumentException for null input");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }

        try {
            TableNameFormatUtils.formatS3Identifier("   ");
            fail("Expected IllegalArgumentException for null input");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test
    void testNonAlphanumericCharacters() {
        assertEquals("my_table", TableNameFormatUtils.formatS3Identifier("my@table!"));
    }

    @Test
    void testLongInputTruncatedTo255() {
        String longInput = "a".repeat(300);
        String result = TableNameFormatUtils.formatS3Identifier(longInput);
        assertEquals(255, result.length());
        assertTrue(result.matches("a+")); // only 'a's remain
    }
}
