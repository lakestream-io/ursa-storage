/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class DeltaVariantUtilsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testVariantPrimitiveRoundTrip() throws Exception {
        assertVariantRoundTrip("\"hello variant\"");
        assertVariantRoundTrip("\"\"");
        assertVariantRoundTrip("0");
        assertVariantRoundTrip("-1");
        assertVariantRoundTrip("123");
        assertVariantRoundTrip("1234567890123");
        assertVariantRoundTrip("-1234567890123");
        assertVariantRoundTrip("true");
        assertVariantRoundTrip("false");
        assertVariantRoundTrip("3.14159");
        assertVariantRoundTrip("-99999.125");
    }

    @Test
    void testVariantNullRoundTrip() throws Exception {
        GenericRow variant = DeltaVariantUtils.fromJson("null");
        assertNotNull(variant);

        String roundTrip = DeltaVariantUtils.deserializeToJsonString(
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.METADATA)),
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.VALUE)));

        assertEquals("null", roundTrip);
        assertNull(DeltaVariantUtils.deserializeString(
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.METADATA)),
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.VALUE))));
    }

    @Test
    void testVariantComplexObjectRoundTrip() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("name", "alice");
        root.put("age", 30);
        root.put("enabled", true);

        ObjectNode nested = root.putObject("metadata");
        nested.put("city", "shanghai");
        nested.putArray("scores").add(98).add(87).add(91);

        root.putArray("events")
                .add("created")
                .addObject()
                .put("type", "updated")
                .put("count", 2);

        assertVariantRoundTrip(root.toString());

        GenericRow variant = DeltaVariantUtils.fromJson(root.toString());
        byte[] metadataBytes = variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.METADATA));
        assertTrue(metadataBytes.length > 2, "Object variant should carry non-empty metadata dictionary");
    }

    @Test
    void testVariantArrayRoundTrip() throws Exception {
        assertVariantRoundTrip("[1,\"two\",true,null,{\"k\":\"v\"},[3,4,5]]");
    }

    @Test
    void testVariantDeepNestedObjectRoundTrip() throws Exception {
        String json = """
                {
                  "profile": {
                    "user": {
                      "id": 7,
                      "name": "alice",
                      "active": true,
                      "addresses": [
                        {
                          "type": "home",
                          "geo": {
                            "lat": 37.77,
                            "lon": -122.41,
                            "history": [
                              {"year": 2022, "ok": true},
                              {"year": 2023, "ok": false, "reasons": ["moved", "updated"]}
                            ]
                          }
                        },
                        {
                          "type": "work",
                          "geo": {
                            "lat": 40.71,
                            "lon": -74.1,
                            "history": []
                          }
                        }
                      ]
                    }
                  },
                  "meta": {
                    "tags": ["gold", "beta"],
                    "scores": [1, 2, {"nested": [3, 4, {"final": "x"}]}]
                  }
                }
                """;

        assertVariantRoundTrip(json);
    }

    @Test
    void testVariantDeepNestedArrayRoundTrip() throws Exception {
        String json = """
                [
                  1,
                  {
                    "events": [
                      {
                        "type": "login",
                        "devices": ["ios", "web"]
                      },
                      {
                        "type": "purchase",
                        "items": [
                          {"sku": "a1", "qty": 2},
                          {"sku": "b9", "qty": 1, "extra": {"promo": true, "codes": ["x", "y"]}}
                        ]
                      }
                    ]
                  },
                  [
                    2,
                    3,
                    {
                      "levels": [
                        {"depth": 1},
                        {"depth": 2, "child": {"depth": 3, "leaf": [true, false, null]}}
                      ]
                    }
                  ],
                  "tail"
                ]
                """;

        assertVariantRoundTrip(json);
    }

    @Test
    void testVariantNumericEdgeCasesRoundTrip() throws Exception {
        String json = """
                {
                  "tiny": 1,
                  "negativeTiny": -1,
                  "small": 32000,
                  "negativeSmall": -32000,
                  "intMaxish": 2147483000,
                  "intMinish": -2147483000,
                  "longValue": 922337203685477000,
                  "negativeLongValue": -922337203685477000,
                  "floatLike": 1.25,
                  "negativeFloatLike": -1.25,
                  "bigDouble": 1234567890.1234567
                }
                """;

        assertVariantRoundTrip(json);
    }

    @Test
    void testVariantBinaryRoundTrip() throws Exception {
        byte[] bytes = "hello-delta-variant".getBytes();
        String base64 = Base64.getEncoder().encodeToString(bytes);
        ObjectNode root = MAPPER.createObjectNode();
        root.put("payload", base64);

        GenericRow variant = DeltaVariantUtils.fromJsonNode(root);
        String roundTrip = DeltaVariantUtils.deserializeToJsonString(
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.METADATA)),
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.VALUE)));

        JsonNode result = MAPPER.readTree(roundTrip);
        assertEquals(base64, result.get("payload").asText());
    }

    @Test
    void testVariantMixedPrimitiveObjectArrayRoundTrip() throws Exception {
        String json = """
                {
                  "string": "value",
                  "emptyString": "",
                  "boolean": true,
                  "number": 42,
                  "decimal": 12.75,
                  "nullField": null,
                  "object": {
                    "inner": {
                      "flag": false,
                      "arr": [1, "two", null, {"deep": [3, 4, {"v": "x"}]}]
                    }
                  },
                  "topArray": [
                    {"kind": "a", "payload": {"x": 1}},
                    {"kind": "b", "payload": [1, 2, 3]},
                    "done"
                  ]
                }
                """;

        assertVariantRoundTrip(json);
    }

    private void assertVariantRoundTrip(String json) throws Exception {
        GenericRow variant = DeltaVariantUtils.fromJson(json);
        assertNotNull(variant);

        String roundTrip = DeltaVariantUtils.deserializeToJsonString(
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.METADATA)),
                variant.getBinary(variant.getSchema().indexOf(DeltaVariantUtils.VALUE)));

        assertEquals(MAPPER.readTree(json), MAPPER.readTree(roundTrip));
    }
}
