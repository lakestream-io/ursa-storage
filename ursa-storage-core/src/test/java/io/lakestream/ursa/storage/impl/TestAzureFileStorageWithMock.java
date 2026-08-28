/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.FileStorage.LIFECYCLE_RULE_ID_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.resourcemanager.storage.fluent.ManagementPoliciesClient;
import com.azure.resourcemanager.storage.fluent.StorageManagementClient;
import com.azure.resourcemanager.storage.fluent.models.ManagementPolicyInner;
import com.azure.resourcemanager.storage.implementation.ManagementPoliciesImpl;
import com.azure.resourcemanager.storage.models.BlobTypes;
import com.azure.resourcemanager.storage.models.DateAfterModification;
import com.azure.resourcemanager.storage.models.ManagementPolicies;
import com.azure.resourcemanager.storage.models.ManagementPolicy;
import com.azure.resourcemanager.storage.models.ManagementPolicyAction;
import com.azure.resourcemanager.storage.models.ManagementPolicyBaseBlob;
import com.azure.resourcemanager.storage.models.ManagementPolicyDefinition;
import com.azure.resourcemanager.storage.models.ManagementPolicyFilter;
import com.azure.resourcemanager.storage.models.ManagementPolicyName;
import com.azure.resourcemanager.storage.models.ManagementPolicyRule;
import com.azure.resourcemanager.storage.models.ManagementPolicySchema;
import com.azure.resourcemanager.storage.models.RuleType;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class TestAzureFileStorageWithMock {

    private AutoCloseable mocks;
    @Mock
    private StorageManager storageManager;
    @Mock
    private StorageManagementClient storageManagementClient;
    @Mock
    private ManagementPoliciesClient managementPoliciesClient;

    private AzureFileStorage storage;

    private ManagementPolicies policies;
    private Map<Pair<String, String>, ManagementPolicyInner> policiesMap = new HashMap<>();

    private static class NotFound extends HttpResponse {

        static NotFound instance = new NotFound(null);

        protected NotFound(HttpRequest request) {
            super(request);
        }

        @Override
        public int getStatusCode() {
            return 404;
        }

        @Override
        public String getHeaderValue(String s) {
            return "";
        }

        @Override
        public HttpHeaders getHeaders() {
            return null;
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return null;
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return null;
        }

        @Override
        public Mono<String> getBodyAsString() {
            return null;
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return null;
        }
    }

    @BeforeEach
    void setup() {
        this.mocks = MockitoAnnotations.openMocks(this);

        this.storageManager = mock(StorageManager.class);
        this.storage = new AzureFileStorage(storageManager);

        // mock the behavior of the storageManager
        doReturn(managementPoliciesClient).when(storageManagementClient).getManagementPolicies();
        doReturn(storageManagementClient).when(storageManager).serviceClient();
        this.policies = spy(new ManagementPoliciesImpl(storageManager));
        doReturn(policies).when(storageManager).managementPolicies();

        when(managementPoliciesClient.getAsync(anyString(), anyString(), eq(ManagementPolicyName.DEFAULT)))
            .thenAnswer(invocation -> {
                String resourceGroupName = invocation.getArgument(0);
                String storageAccountName = invocation.getArgument(1);
                Pair<String, String> key = Pair.of(resourceGroupName, storageAccountName);
                var policy = policiesMap.get(key);
                if (policy == null) {
                    return Mono.fromFuture(CompletableFuture.failedFuture(
                        new ManagementException("not found", NotFound.instance)));
                }
                return Mono.fromFuture(CompletableFuture.completedFuture(policy));
            });

        when(managementPoliciesClient.createOrUpdateAsync(anyString(), anyString(),
            eq(ManagementPolicyName.DEFAULT), any(ManagementPolicyInner.class)))
            .thenAnswer(invocation -> {
                String resourceGroupName = invocation.getArgument(0);
                String storageAccountName = invocation.getArgument(1);
                ManagementPolicyInner policyInner = invocation.getArgument(3);
                Pair<String, String> key = Pair.of(resourceGroupName, storageAccountName);
                policiesMap.put(key, policyInner);
                return Mono.fromFuture(CompletableFuture.completedFuture(policyInner));
            });
    }

    @AfterEach
    void cleanup() throws Exception {
        mocks.close();
    }

    @Test
    void testCreateRules() throws Exception {
        storage.deleteWithDatePrefixes(Set.of("test")).get();

        var p = policiesMap.values().stream().toList();
        assertEquals(1, p.size());
        var mpi = p.get(0);
        var rules = mpi.policy().rules();
        assertEquals(1, rules.size());
        var rule = rules.get(0);
        assertTrue(rule.name().startsWith(LIFECYCLE_RULE_ID_PREFIX));
        var prefixes = rule.definition().filters().prefixMatch();
        assertEquals(1, prefixes.size());
        assertEquals(storage.getBucketPrefix() + "/test", prefixes.get(0));
    }

    @Test
    void createMoreRules() throws Exception {
        Set<String> prefixes = new HashSet<>();

        for (int i = 0; i < 22; i++) {
            prefixes.add("prefix" + i);
        }

        storage.deleteWithDatePrefixes(prefixes).get();

        var p = policiesMap.values().stream().toList();
        assertEquals(1, p.size());
        var mpi = p.get(0);
        var rules = mpi.policy().rules();
        assertEquals(3, rules.size());
        for (int i = 0; i < 3; i++) {
            var rule = rules.get(i);
            assertTrue(rule.name().startsWith(LIFECYCLE_RULE_ID_PREFIX));
            var pfx = rule.definition().filters().prefixMatch();
            for (String s : pfx) {
                prefixes.remove(s.substring(storage.getBucketPrefix().length() + 1));
            }
        }
        assertEquals(0, prefixes.size());
    }

    @Test
    void removeExpiredPrefixes() throws Exception {
        IDGenerator idGenerator = IDGenerator.create("dateuuid", "", null);

        // mock prefixes
        Set<String> prefixes = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            prefixes.add(IDGeneratorWithDate.getDatePrefix(LocalDateTime.now().minusDays(8).minusHours(i)));
        }
        var rules = storage.getUrsaRules(prefixes);
        assertEquals(0, rules.size());
    }

    @Test
    void updateRulesWithEmptyRules() throws Exception {
        ManagementPolicy policy = mock(ManagementPolicy.class);
        ManagementPolicySchema schema = new ManagementPolicySchema();
        schema.withRules(Collections.emptyList());
        when(policy.policy()).thenReturn(schema);


        var prefixes = getPrefixes(10, LocalDateTime.now());
        var updatedSchema = storage.updatePolicySchemaByPrefixes(policy, prefixes);
        assertEquals(1, updatedSchema.rules().size());
        assertEquals(10, updatedSchema.rules().get(0).definition().filters().prefixMatch().size());
        checkRulesAreExpected(prefixes, updatedSchema, 0);

        var prefixes2 = getPrefixes(5, LocalDateTime.now().minusDays(1));
        updatedSchema = storage.updatePolicySchemaByPrefixes(policy, prefixes2);
        assertEquals(2, updatedSchema.rules().size());

        Set<String> allPrefixes = new HashSet<>();
        allPrefixes.addAll(prefixes2);
        allPrefixes.addAll(prefixes);
        checkRulesAreExpected(allPrefixes, updatedSchema, 0);

    }

    @Test
    void updateRulesWithExistingRules() throws Exception {
        ManagementPolicy policy = mock(ManagementPolicy.class);
        ManagementPolicySchema schema = new ManagementPolicySchema();
        List<ManagementPolicyRule> rules = new ArrayList<>();
        int otherRules = 2;
        for (int i = 0; i < otherRules; i++) {
            ManagementPolicyRule rule = new ManagementPolicyRule();
            rule.withName("rule-" + i)
                .withEnabled(true)
                .withType(RuleType.LIFECYCLE)
                .withDefinition(new ManagementPolicyDefinition()
                    .withActions(new ManagementPolicyAction()
                        .withBaseBlob(new ManagementPolicyBaseBlob()
                            .withDelete(new DateAfterModification()
                                .withDaysAfterModificationGreaterThan(0f))))
                    .withFilters(new ManagementPolicyFilter()
                        .withBlobTypes(List.of(BlobTypes.BLOCK_BLOB.toString()))
                        .withPrefixMatch(List.of("prefix-" + i))));
            rules.add(rule);
        }
        schema.withRules(rules);
        when(policy.policy()).thenReturn(schema);

        var prefixes = getPrefixes(10, LocalDateTime.now());
        var updatedSchema = storage.updatePolicySchemaByPrefixes(policy, prefixes);
        assertEquals(3, updatedSchema.rules().size());
        checkRulesAreExpected(prefixes, updatedSchema, otherRules);
    }

    void checkRulesAreExpected(Set<String> prefixes, ManagementPolicySchema schema, int otherRules) {
        Set<String> expectedPrefixes = new HashSet<>(prefixes);
        Set<String> prefixesInRules = new HashSet<>();
        for (ManagementPolicyRule rule : schema.rules()) {
            for (String prefix : rule.definition().filters().prefixMatch()) {
                prefixesInRules.add(prefix);
                var p = prefix.replace(storage.getBucketPrefix() + "/", "");
                expectedPrefixes.remove(p);
            }
        }
        assertEquals(prefixes.size(), prefixesInRules.size() - otherRules, prefixesInRules.toString());
        assertEquals(0, expectedPrefixes.size());
    }

    Set<String> getPrefixes(int n, LocalDateTime dateTime) throws Exception {
        IDGenerator idGenerator = IDGenerator.create("dateuuid", "", null);

        // mock prefixes
        Set<String> prefixes = new HashSet<>();
        for (int i = 0; i < n; i++) {
            prefixes.add(IDGeneratorWithDate.getDatePrefix(dateTime.minusHours(i)));
        }
        return prefixes;
    }

}
