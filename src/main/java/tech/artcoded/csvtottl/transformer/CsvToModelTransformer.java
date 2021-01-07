package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Service
@Slf4j
public class CsvToModelTransformer implements CommandLineRunner {
    private static final String NAMESPACE_PREFIX = "http://bittich.be/bce";
    private static final org.apache.jena.rdf.model.Resource CODE_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/code");
    @Value("classpath:code.csv")
    private Resource codeCsv;
    @Value("classpath:enterprise.csv")
    private Resource entrepriseCsv;
    @Value("classpath:denomination.csv")
    private Resource denominationCsv;
    @Value("${virtuoso.username:dba}")
    private String username;
    @Value("${virtuoso.password:dba}")
    private String password;
    @Value("${virtuoso.defaultGraphUri:http://mu.semte.ch/application}")
    private String defaultGraphUri;
    @Value("${virtuoso.host:http://localhost:8890}")
    private String host;

    @SneakyThrows
    public Model transform() {
        Model model = ModelFactory.createDefaultModel();
        transformCodes(model);
        transformEntreprises(model);
        transformDenominations(model);
        return model;


    }

    @SneakyThrows
    private void transformDenominations(Model model) {
        log.info("create denomination turtle file...it takes a while :-(");


        List<Map<String, String>> csvDdenominations = CSVReaderUtils.readMap(denominationCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvDdenominations.stream().
                collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Denomination", map.get("EntityNumber").replaceAll("\\.", ""))));

        groupedResources.forEach((key, value) -> {
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.stream().map(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Organization", v.get("EntityNumber").replaceAll("\\.", ""));
                DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/org"), organizationUri, List.of(ResourceFactory.createResource(NAMESPACE_PREFIX + "/enterprise")), false);
                String lang = switch (v.get("Language")) {
                    case "2" -> "nl";
                    case "3" -> "de";
                    case "4" -> "en";
                    default -> "fr";
                };
                return Map.entry(v.get("Denomination"), lang);
            })
                    .filter(entry -> StringUtils.isNotEmpty(entry.getKey()) && StringUtils.isNotEmpty(entry.getValue()))
                    .forEach(entry -> {
                        DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/name"), entry.getValue(), entry.getKey(), false);
                    });
            model.add(resource, RDF.type, ResourceFactory.createResource(NAMESPACE_PREFIX + "/denomination"));
        });
    }


    @SneakyThrows
    private void transformEntreprises(Model model) {
        log.info("create enterprise turtle file...");

        List<Map<String, String>> csvEnterprises = CSVReaderUtils.readMap(entrepriseCsv.getInputStream());


        Map<String, List<Map<String, String>>> groupedResources = csvEnterprises.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Organization", map.get("EnterpriseNumber").replaceAll("\\.", ""))));


        groupedResources.forEach((key, value) -> {
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(line -> {
                ofNullable(line.get("Status")).filter(StringUtils::isNotEmpty).map(statusCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Status", statusCode))
                        .ifPresentOrElse(statusUri -> {
                            DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/status"), statusUri, List.of(CODE_TYPE), false);
                        }, () -> log.trace("'status' not found"));

                ofNullable(line.get("JuridicalSituation"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jsCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalSituation", jsCode))
                        .ifPresentOrElse(juridicalSituationUri -> {
                            DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/juridicalsituation"), juridicalSituationUri, List.of(CODE_TYPE), false);
                        }, () -> log.trace("'juridicalSituationUri' not found"));

                ofNullable(line.get("TypeOfEnterprise"))
                        .filter(StringUtils::isNotEmpty)
                        .map(typeOfEntrepriseCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "TypeOfEnterprise", typeOfEntrepriseCode))
                        .ifPresentOrElse(typeOfEntrepriseUri -> {
                            DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/typeofenterprise"), typeOfEntrepriseUri, List.of(CODE_TYPE), false);
                        }, () -> log.trace("'typeOfEntrepriseUri' not found"));


                ofNullable(line.get("JuridicalForm"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jfCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalForm", jfCode))

                        .ifPresentOrElse(juridicalFormUri -> {
                            DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/juridicalform"), juridicalFormUri, List.of(CODE_TYPE), false);
                        }, () -> log.trace("'juridicalFormUri' not found"));

                DataTransformer.addDateValue(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/startdate"), line.get("StartDate"), false);

            });
            model.add(resource, RDF.type, ResourceFactory.createResource(NAMESPACE_PREFIX + "/enterprise"));

        });

    }

    @SneakyThrows
    private void transformCodes(Model model) {
        log.info("create codes turtle file...");

        List<Map<String, String>> csvCodes = CSVReaderUtils.readMap(codeCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvCodes.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, map.get("Category"), map.get("Code"))));

        groupedResources.forEach((key, value) -> {
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(lang -> DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/description"), lang.get("Description"), lang.get("Language").toLowerCase(), false));
            model.add(resource, RDF.type, CODE_TYPE);
        });

    }

    @Override
    public void run(String... args) throws Exception {
        log.info("start migration...");
        Model model = this.transform();
        log.info("perform save...");
        RDFDataMgr.write(new FileOutputStream("/tmp/bce.ttl"), model, RDFFormat.TURTLE_BLOCKS);
        log.info("migration done.");

    }
}
