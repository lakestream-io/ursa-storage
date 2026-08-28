/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.pojo;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexPerson {

    private Address address;
    private String firstName;
    private String lastName;
    private String email;
    private String username;
    private String password;
    private Sex sex;
    private String telephoneNumber;
    private long dateOfBirth;
    private Integer age;
    private Company company;
    private Byte byteValue;
    private byte[] bytesValue;
    private Integer[] intsValue;
    private List<String> stringListValue;
    private Short shortValue;
    private Double doubleValue;
    private Float floatValue;
    private Boolean booleanValue;
    Map<String, String> mapValue;
    Map<String, Company> companyMap;
    List<Company> companyList;
    private Instant instant;
    private LocalDate localDate;
    private LocalTime localTime;
    private LocalDateTime localDateTime;

    public enum Sex {
        MALE,
        FEMALE;

        Sex() {
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Company {
        private String name;
        private String domain;
        private String email;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Address {
        private String street;
        private String streetNumber;
        private String apartmentNumber;
        private String postalCode;
        private String city;
    }

    public static ComplexPerson newComplexPerson(Instant instant) {
        ComplexPerson complexPerson = new ComplexPerson();

        Address address = new Address();
        address.setCity("city");
        address.setStreet("street");
        address.setApartmentNumber("apartmentNumber");
        address.setPostalCode("postalCode");
        address.setStreetNumber("streetNumber");
        complexPerson.setAddress(address);

        complexPerson.setFirstName("firstName");
        complexPerson.setLastName("lastName");
        complexPerson.setEmail("email");
        complexPerson.setUsername("username");
        complexPerson.setPassword("password");
        complexPerson.setSex(Sex.MALE);
        complexPerson.setTelephoneNumber("telephoneNumber");
        complexPerson.setDateOfBirth(0L);
        complexPerson.setAge(0);

        Company company = new Company();
        company.setAddress(address);
        company.setEmail("email");
        company.setDomain("domain");
        company.setName("name");
        complexPerson.setCompany(company);

        complexPerson.setByteValue((byte) 0);
        complexPerson.setBytesValue(new byte[]{(byte) 0, (byte) 1, (byte) 2, (byte) 3});
        complexPerson.setIntsValue(new Integer[]{1, 2, 3});
        complexPerson.setStringListValue(Arrays.asList("1", "2", "3"));
        complexPerson.setShortValue((short) 0);
        complexPerson.setDoubleValue(0.0d);
        complexPerson.setFloatValue(0.0f);
        complexPerson.setBooleanValue(true);
        Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        complexPerson.setMapValue(map);

        Map<String, Company> companyMap = new HashMap<>();
        companyMap.put("a", company);
        complexPerson.setCompanyMap(companyMap);
        complexPerson.setCompanyList(List.of(company));
        complexPerson.setInstant(instant);

        LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
        LocalTime localTime = instant.atZone(ZoneId.systemDefault()).toLocalTime();
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        complexPerson.setLocalDate(localDate);
        complexPerson.setLocalTime(localTime);
        complexPerson.setLocalDateTime(localDateTime);
        return complexPerson;

    }
}
