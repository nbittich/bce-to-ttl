package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import tech.artcoded.csvtottl.utils.VirtuosoUploadUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static tech.artcoded.csvtottl.transformer.DataTransformer.addResource;

@Service
@Slf4j
public class CsvToModelTransformer implements CommandLineRunner {
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
    public void transform() {
        transformCodes();
        transformEntreprises();
        transformContacts();
        transformDenominations();
    }

    @SneakyThrows
    private void transformDenominations() {
        log.info("create denomination turtle file...it takes a while :-(");


        List<Map<String, String>> csvDdenominations = CSVReaderUtils.readMap(denominationCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvDdenominations.stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));

        groupedResources.forEach((key, value) -> {
            Model model = ModelFactory.createDefaultModel();

            org.apache.jena.rdf.model.Resource resource = model.createResource("%s/%s/%s".formatted(NAMESPACE_PREFIX, "denomination", key));

            var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", key);
            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasOrganization"), organizationUri, List.of(ORG_TYPE), false);

            org.apache.jena.rdf.model.Resource labelResource = model.createResource(NAMESPACE_PREFIX + "/label");

            value.stream().map(v -> {

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
                        DataTransformer.addLangLiteral(labelResource, RDFS.label, entry.getKey(), entry.getValue(), false);
                    });
            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasLabel"), labelResource.getURI(), List.of(RDFS.label), false);
            model.add(resource, RDF.type, DENOMINATION_TYPE);
            VirtuosoUploadUtils.upload(model,defaultGraphUri,host, username, password);

        });
    }

    @SneakyThrows
    private void transformContacts() {
        log.info("create contact turtle file...");

        List<Map<String, String>> csvContacts = CSVReaderUtils.readMap(contactCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvContacts.stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));

        groupedResources.forEach((key, value) -> {
            Model model = ModelFactory.createDefaultModel();

            org.apache.jena.rdf.model.Resource resource = model.createResource("%s/%s/%s".formatted(NAMESPACE_PREFIX, "contact", key));
            var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", key);

            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasOrganization"), organizationUri, List.of(ORG_TYPE), false);

            value.forEach(v -> {
                ofNullable(v.get("ContactType")).filter(StringUtils::isNotEmpty).map(ct -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "ContactType", ct))
                        .ifPresentOrElse(uri -> {
                            org.apache.jena.rdf.model.Resource contactTypeResource = model.createResource(uri);
                            DataTransformer.addLiteral(contactTypeResource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/value"), v.get("Value"), false);
                            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasContactType"), contactTypeResource.getURI(), List.of(CODE_TYPE), false);
                        }, () -> log.trace("'contact type' not found"));

            });


            model.add(resource, RDF.type, CONTACT_TYPE);
            VirtuosoUploadUtils.upload(model,defaultGraphUri,host, username, password);

        });
    }


    @SneakyThrows
    private void transformEntreprises() {
        log.info("create enterprise turtle file...");

        List<Map<String, String>> csvEnterprises = CSVReaderUtils.readMap(entrepriseCsv.getInputStream());


        Map<String, List<Map<String, String>>> groupedResources = csvEnterprises.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", map.get("EnterpriseNumber").replaceAll("\\.", ""))));


        groupedResources.forEach((key, value) -> {
            Model model = ModelFactory.createDefaultModel();

            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(line -> {
                ofNullable(line.get("Status")).filter(StringUtils::isNotEmpty).map(statusCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Status", statusCode))
                        .ifPresentOrElse(statusUri -> {
                            org.apache.jena.rdf.model.Resource statusResource = model.createResource(statusUri);
                            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStatus"), statusResource.getURI(), List.of(CODE_TYPE), false);
                        }, () -> log.trace("'status' not found"));

                ofNullable(line.get("JuridicalSituation"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jsCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalSituation", jsCode))
                        .ifPresentOrElse(juridicalSituationUri -> {
                            org.apache.jena.rdf.model.Resource jsResource = model.createResource(juridicalSituationUri);
                            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalSituation"), jsResource.getURI(), List.of(CODE_TYPE), false);
                        }, () -> log.trace("'juridicalSituationUri' not found"));

                ofNullable(line.get("TypeOfEnterprise"))
                        .filter(StringUtils::isNotEmpty)
                        .map(typeOfEntrepriseCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "TypeOfEnterprise", typeOfEntrepriseCode))
                        .ifPresentOrElse(typeOfEntrepriseUri -> {
                            org.apache.jena.rdf.model.Resource toeResource = model.createResource(typeOfEntrepriseUri);
                            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasTypeOfCompany"), toeResource.getURI(), List.of(CODE_TYPE), false);
                        }, () -> log.trace("'typeOfEntrepriseUri' not found"));


                ofNullable(line.get("JuridicalForm"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jfCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalForm", jfCode))

                        .ifPresentOrElse(juridicalFormUri -> {
                            org.apache.jena.rdf.model.Resource jfResource = model.createResource(juridicalFormUri);
                            addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalForm"), jfResource.getURI(), List.of(CODE_TYPE), false);
                        }, () -> log.trace("'juridicalFormUri' not found"));

                DataTransformer.addLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStartDate"), line.get("StartDate"), false);

            });
            model.add(resource, RDF.type, ORG_TYPE);
            VirtuosoUploadUtils.upload(model,defaultGraphUri,host, username, password);

        });

    }

    @SneakyThrows
    private void transformCodes() {
        log.info("create codes turtle file...");

        List<Map<String, String>> csvCodes = CSVReaderUtils.readMap(codeCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvCodes.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, map.get("Category"), map.get("Code"))));

        groupedResources.forEach((key, value) -> {
            Model model = ModelFactory.createDefaultModel();
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(lang -> DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasDescription"), lang.get("Description"), lang.get("Language").toLowerCase(), false));
            model.add(resource, RDF.type, CODE_TYPE);
            VirtuosoUploadUtils.upload(model,defaultGraphUri,host, username, password);
        });

    }

    @Override
    public void run(String... args) throws Exception {
        log.info("start migration...");
        this.transform();
        //RDFDataMgr.write(new FileOutputStream("/tmp/bce.ttl"), model, RDFFormat.TURTLE);
        log.info("migration done.");

    }
}
