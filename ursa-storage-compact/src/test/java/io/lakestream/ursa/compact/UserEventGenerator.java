/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class UserEventGenerator {

    private static final String[] TAGS = {"sports", "music", "gaming", "travel", "food", "tech", "art", "photography"};
    private static final String[] CITIES = {"New York", "San Francisco", "Los Angeles", "Chicago", "Seattle", "Austin"};
    private static final String[] STREETS = {"Main St", "Elm St", "Oak St", "Pine St", "Maple Ave", "Broadway"};
    private static final String[] EMAIL_DOMAINS = {"example.com", "mail.com", "test.org"};

    private static String randomString(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String randomTag() {
        return TAGS[ThreadLocalRandom.current().nextInt(TAGS.length)];
    }

    private static String randomCity() {
        return CITIES[ThreadLocalRandom.current().nextInt(CITIES.length)];
    }

    private static String randomStreet() {
        return ThreadLocalRandom.current().nextInt(1, 999)
               + " " + STREETS[ThreadLocalRandom.current().nextInt(STREETS.length)];
    }

    private static String randomEmail(String userId) {
        String domain = EMAIL_DOMAINS[ThreadLocalRandom.current().nextInt(EMAIL_DOMAINS.length)];
        return userId.toLowerCase() + "@" + domain;
    }

    private static Map<String, String> randomAttributes() {
        Map<String, String> map = new HashMap<>();
        map.put("lang", List.of("en", "fr", "es").get(ThreadLocalRandom.current().nextInt(3)));
        map.put("tier", List.of("bronze", "silver", "gold", "platinum").get(ThreadLocalRandom.current().nextInt(4)));
        return map;
    }

    public static UserEventV1 randomV1(int index) {
        String id = "user-v1-" + index;
        return new UserEventV1(
            id,
            ThreadLocalRandom.current().nextInt(18, 70),
            ThreadLocalRandom.current().nextBoolean(),
            ThreadLocalRandom.current().nextDouble(0, 100),
            List.of(randomTag(), randomTag()),
            randomAttributes(),
            new UserEventV1.Address(randomStreet(), randomCity())
        );
    }

    public static UserEventWithVariantV1 randomVariantV1(int index) {
        String id = "user-v1-" + index;
        return new UserEventWithVariantV1(
                id,
                ThreadLocalRandom.current().nextInt(18, 70),
                ThreadLocalRandom.current().nextBoolean(),
                ThreadLocalRandom.current().nextDouble(0, 100),
                List.of(randomTag(), randomTag()),
                randomAttributes(),
                new UserEventWithVariantV1.Address(randomStreet(), randomCity())
        );
    }

    public static TestProtoVariant.UserEventWithVariant randomUserEventWithVariant(int index) {
        String id = "user-v1-" + index;

        TestProtoVariant.UserEventWithVariant.Builder builder = TestProtoVariant.UserEventWithVariant.newBuilder();
        return builder.setId(id)
                .setAge(ThreadLocalRandom.current().nextInt(18, 70))
                .setActive(ThreadLocalRandom.current().nextBoolean())
                .setScore(ThreadLocalRandom.current().nextDouble(0, 100))
                .addTags(randomTag())
                .addTags(randomTag())
                .putAllAttributes(randomAttributes())
                .setAddress(
                        TestProtoVariant.Address.newBuilder()
                                .setStreet(randomStreet())
                                .setCity(randomCity())
                                .build()
                ).build();
    }

    public static UserEventV2 randomV2(int index) {
        String id = "user-v2-" + index;
        return new UserEventV2(
            id,
            ThreadLocalRandom.current().nextLong(18, 70),
            ThreadLocalRandom.current().nextBoolean(),
            ThreadLocalRandom.current().nextDouble(0, 100),
            List.of(randomTag(), randomTag()),
            randomAttributes(),
            randomEmail(id),
            new UserEventV2.Address(randomStreet(), randomCity())
        );
    }

    public static UserEventV3 randomV3(int index) {
        String id = "user-v3-" + index;
        return new UserEventV3(
            id,
            ThreadLocalRandom.current().nextLong(18, 70),
            ThreadLocalRandom.current().nextBoolean(),
            ThreadLocalRandom.current().nextDouble(0, 100),
            List.of(randomTag(), randomTag(), randomTag()),
            randomAttributes(),
            randomEmail(id),
            new UserEventV3.Address(randomStreet(), randomCity(),
                String.valueOf(ThreadLocalRandom.current().nextInt(10000, 99999)))
        );
    }

    public static List<UserEventV1> generateV1List(int count) {
        List<UserEventV1> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(randomV1(i));
        }
        return list;
    }

    public static List<UserEventV2> generateV2List(int count) {
        List<UserEventV2> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(randomV2(i));
        }
        return list;
    }

    public static List<UserEventV3> generateV3List(int count) {
        List<UserEventV3> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(randomV3(i));
        }
        return list;
    }

    public static void main(String[] args) {
        System.out.println("Generating 100 random V1 instances...");
        List<UserEventV1> v1List = generateV1List(100);
        v1List.stream().limit(3).forEach(System.out::println);

        System.out.println("\nGenerating 100 random V2 instances...");
        List<UserEventV2> v2List = generateV2List(100);
        v2List.stream().limit(3).forEach(System.out::println);

        System.out.println("\nGenerating 100 random V3 instances...");
        List<UserEventV3> v3List = generateV3List(100);
        v3List.stream().limit(3).forEach(System.out::println);
    }
}
