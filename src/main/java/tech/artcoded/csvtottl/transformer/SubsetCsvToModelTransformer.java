package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;

import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

//@Service
@Slf4j
public class SubsetCsvToModelTransformer implements CommandLineRunner {
    private static final String NAMESPACE_PREFIX = "http://bittich.be/bce";
    private static final org.apache.jena.rdf.model.Resource CODE_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Code");
    private static final org.apache.jena.rdf.model.Resource DENOMINATION_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Denomination");
    private static final org.apache.jena.rdf.model.Resource CONTACT_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Contact");

    @Value("classpath:code.csv")
    private Resource codeCsv;
    @Value("classpath:enterprise-subset.csv")
    private Resource entrepriseCsv;
    @Value("classpath:denomination-subset.csv")
    private Resource denominationCsv;
    @Value("classpath:contact-subset.csv")
    private Resource contactCsv;

    @SneakyThrows
    public Model transform() {
        Model model = ModelFactory.createDefaultModel();
        transformCodes(model);
        transformEntreprises(model);
        transformContacts(model);
        transformDenominations(model);
        return model;
    }

    @SneakyThrows
    private void transformDenominations(Model model) {
        log.info("create denomination turtle file...it takes a while :-(");


        List<Map<String, String>> csvDdenominations = CSVReaderUtils.readMap(denominationCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvDdenominations.stream().
                collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "denomination", map.get("EntityNumber").replaceAll("\\.", ""))));

        groupedResources.forEach((key, value) -> {

            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.stream().map(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", v.get("EntityNumber").replaceAll("\\.", ""));
                DataTransformer.addResource(resource, ORG.hasUnit, organizationUri, List.of(ORG.Organization), false);
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
                        DataTransformer.addLangLiteral(resource, FOAF.name, entry.getKey(), entry.getValue(), false);
                    });
            model.add(resource, RDF.type, DENOMINATION_TYPE);

        });
    }

    @SneakyThrows
    private void transformContacts(Model model) {
        log.info("create contact turtle file...");


        List<Map<String, String>> csvContacts = CSVReaderUtils.readMap(contactCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvContacts.stream().
                collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "contact", map.get("EntityNumber").replaceAll("\\.", ""))));

        groupedResources.forEach((key, value) -> {

            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", v.get("EntityNumber").replaceAll("\\.", ""));
                DataTransformer.addResource(resource, ORG.hasUnit, organizationUri, List.of(ORG.Organization), false);

                ofNullable(v.get("ContactType")).filter(StringUtils::isNotEmpty)
                        .ifPresentOrElse(contactType -> {
                            if("EMAIL".equals(contactType)){
                                model.addLiteral(resource, FOAF.mbox, ResourceFactory.createStringLiteral(v.get("Value")));
                            }else if("WEB".equals(contactType)){
                                model.addLiteral(resource, FOAF.homepage, ResourceFactory.createStringLiteral(v.get("Value")));
                            }else if("TEL".equals(contactType)){
                                model.addLiteral(resource, FOAF.phone, ResourceFactory.createStringLiteral(v.get("Value")));
                            }

                        }, () -> log.trace("'contact type' not found"));

            });


            model.add(resource, RDF.type, CONTACT_TYPE);

        });
    }


    @SneakyThrows
    private void transformEntreprises(Model model) {

        List<Map<String, String>> csvEnterprises = CSVReaderUtils.readMap(entrepriseCsv.getInputStream());


        Map<String, List<Map<String, String>>> groupedResources = csvEnterprises.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", map.get("EnterpriseNumber").replaceAll("\\.", ""))));


        groupedResources.forEach((key, value) -> {

            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(line -> {
                ofNullable(line.get("Status")).filter(StringUtils::isNotEmpty).map(statusCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "Status", statusCode))
                        .ifPresentOrElse(statusUri -> {
                            org.apache.jena.rdf.model.Resource js = model.getResource(statusUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStatus"), js);
                        }, () -> log.trace("'status' not found"));

                ofNullable(line.get("JuridicalSituation"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jsCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalSituation", jsCode))
                        .ifPresentOrElse(juridicalSituationUri -> {
                            org.apache.jena.rdf.model.Resource js = model.getResource(juridicalSituationUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalSituation"), js);
                        }, () -> log.trace("'juridicalSituationUri' not found"));

                ofNullable(line.get("TypeOfEnterprise"))
                        .filter(StringUtils::isNotEmpty)
                        .map(typeOfEntrepriseCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "TypeOfEnterprise", typeOfEntrepriseCode))
                        .ifPresentOrElse(typeOfEntrepriseUri -> {
                            org.apache.jena.rdf.model.Resource js = model.getResource(typeOfEntrepriseUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasTypeOfCompany"), js);
                        }, () -> log.trace("'typeOfEntrepriseUri' not found"));


                ofNullable(line.get("JuridicalForm"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jfCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "JuridicalForm", jfCode))

                        .ifPresentOrElse(juridicalFormUri -> {
                            org.apache.jena.rdf.model.Resource js = model.getResource(juridicalFormUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalForm"), js);
                        }, () -> log.trace("'juridicalFormUri' not found"));

                DataTransformer.addLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStartDate"), line.get("StartDate"), false);

            });
            model.add(resource, RDF.type, ORG.Organization);

        });

    }

    @SneakyThrows
    private void transformCodes(Model model) {
        log.info("create codes turtle file...");

        List<Map<String, String>> csvCodes = CSVReaderUtils.readMap(codeCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvCodes.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, map.get("Category"), map.get("Code"))));

        groupedResources.forEach((key, value) -> {
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.forEach(lang -> DataTransformer.addLangLiteral(resource, RDFS.label, lang.get("Description"), lang.get("Language").toLowerCase(), false));
            model.add(resource, RDF.type, CODE_TYPE);
        });

    }

    @Override
    public void run(String... args) throws Exception {
        log.info("start migration...");
        //Model model = this.transform();
       // RDFDataMgr.write(new FileOutputStream("/tmp/bce.ttl"), model, RDFFormat.TURTLE);
        log.info("migration done.");

    }
}

