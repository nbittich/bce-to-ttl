package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;
import tech.artcoded.csvtottl.utils.CsvDto;
import tech.artcoded.csvtottl.utils.VirtuosoUploadUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static tech.artcoded.csvtottl.transformer.DataTransformer.addResource;

@Service
@Slf4j
public class CsvToSubset implements CommandLineRunner {
    private static final String NAMESPACE_PREFIX = "http://bittich.be/bce";
    private static final org.apache.jena.rdf.model.Resource CODE_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Code");
    private static final org.apache.jena.rdf.model.Resource DENOMINATION_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Denomination");
    private static final org.apache.jena.rdf.model.Resource CONTACT_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Contact");
    private static final org.apache.jena.rdf.model.Resource ORG_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Organization");

    @Value("classpath:code.csv")
    private Resource codeCsv;
    @Value("classpath:enterprise.csv")
    private Resource entrepriseCsv;
    @Value("classpath:denomination.csv")
    private Resource denominationCsv;
    @Value("classpath:contact.csv")
    private Resource contactCsv;
    @Value("${virtuoso.username:dba}")
    private String username;
    @Value("${virtuoso.password:dba}")
    private String password;
    @Value("${virtuoso.defaultGraphUri:http://mu.semte.ch/application}")
    private String defaultGraphUri;
    @Value("${virtuoso.host:http://localhost:8890}")
    private String host;

    @SneakyThrows
    public void transform(long size) {
        Set<String> enterpriseNumbers = transformEntreprises(size);
        transformContacts(enterpriseNumbers);
        transformDenominations(enterpriseNumbers);
    }

    @SneakyThrows
    private void transformDenominations(Set<String> enterpriseNumbers) {
        log.info("create denomination csv file...");

        CsvDto csvDdenominations = CSVReaderUtils.readObj(denominationCsv.getInputStream());
        String[] titles = csvDdenominations.getTitles();

        Map<String, List<Map<String, String>>> groupedResources = csvDdenominations.getCsv().stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));

        List<String> list = enterpriseNumbers.stream()
                .map(groupedResources::get)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(m -> "%s,%s,%s,\"%s\"".formatted(m.getOrDefault(titles[0],""), m.getOrDefault(titles[1],""), m.getOrDefault(titles[2],""), m.getOrDefault(titles[3],"")))
                .collect(Collectors.toList());
        List<String> lines = Stream.concat(Stream.of(String.join(",", titles)),list.stream())
                .collect(Collectors.toUnmodifiableList());
        FileUtils.writeLines(new File("/tmp/denomination-subset.csv"), StandardCharsets.UTF_8.name(), lines);
    }

    @SneakyThrows
    private void transformContacts(Set<String> enterpriseNumbers) {
        log.info("create contact csv file...");

        CsvDto csvContacts = CSVReaderUtils.readObj(contactCsv.getInputStream());
        String[] titles = csvContacts.getTitles();

        Map<String, List<Map<String, String>>> groupedResources = csvContacts.getCsv()
                .stream().collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));

        List<String> list = enterpriseNumbers.stream()
                .map(groupedResources::get)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(m -> "%s,%s,%s,%s".formatted(m.getOrDefault(titles[0],""), m.getOrDefault(titles[1],""), m.getOrDefault(titles[2],""), m.getOrDefault(titles[3],"")))
               .collect(Collectors.toList());
        List<String> lines = Stream.concat(Stream.of(String.join(",", titles)),list.stream())
                .collect(Collectors.toUnmodifiableList());
        FileUtils.writeLines(new File("/tmp/contact-subset.csv"), StandardCharsets.UTF_8.name(), lines);


    }


    @SneakyThrows
    private Set<String> transformEntreprises(long size) {
        log.info("create enterprise csv file...");
        CsvDto csvEnterprises = CSVReaderUtils.readObj(entrepriseCsv.getInputStream());
        String[] titles = csvEnterprises.getTitles();
        Map<String, List<String>> csvSubset = csvEnterprises.getCsv().stream()
                .limit(size)
                .collect(Collectors.groupingBy(map ->  map.get("EnterpriseNumber").replaceAll("\\.", "")))
                .entrySet()
                .stream()
                .map(entry ->
                        Map.entry(entry.getKey(), entry.getValue().stream().map(m -> "%s,%s,%s,%s,%s,%s".formatted(m.get(titles[0]),m.get(titles[1]),m.get(titles[2]),m.get(titles[3]),m.get(titles[4]),m.get(titles[5]))).collect(toList()))
                        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                ;
        List<String> lines = Stream.concat(Stream.of(String.join(",", titles)),csvSubset.values().stream().flatMap(Collection::stream)).collect(Collectors.toUnmodifiableList());
        FileUtils.writeLines(new File("/tmp/enterprise-subset.csv"), StandardCharsets.UTF_8.name(), lines);
        return csvSubset.keySet();
    }


    @Override
    public void run(String... args) throws Exception {
       // log.info("start migration...");
     //   this.transform(1000);
       // log.info("migration done.");

    }
}
