package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.ORG;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VCARD;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;
import tech.artcoded.csvtottl.utils.CsvDto;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class CsvToSubset implements CommandLineRunner {
    private static final String NAMESPACE_PREFIX = "http://bittich.be/bce";
    private static final org.apache.jena.rdf.model.Resource CODE_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Code");
    private static final org.apache.jena.rdf.model.Resource DENOMINATION_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Denomination");
    private static final org.apache.jena.rdf.model.Resource CONTACT_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Contact");
    private static final org.apache.jena.rdf.model.Resource ADDR_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/Address");
    private static final Property MU_UUID = ResourceFactory.createProperty("http://mu.semte.ch/vocabularies/core/uuid");

    @Value("classpath:code.csv")
    private Resource codeCsv;
    @Value("classpath:enterprise.csv")
    private Resource entrepriseCsv;
    @Value("classpath:denomination.csv")
    private Resource denominationCsv;
    @Value("classpath:contact.csv")
    private Resource contactCsv;
    @Value("${chunkSize:4000}")
    private int chunkSize;
    @Value("classpath:address.csv")
    private Resource addressCsv;

    public static <T> List<List<T>> getBatches(List<T> collection, int batchSize) {
        return IntStream.iterate(0, i -> i < collection.size(), i -> i + batchSize)
                .mapToObj(i -> collection.subList(i, Math.min(i + batchSize, collection.size())))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    public void transform(int batchSize, File batchDir) {
        Model codesToModel = codesToModel(batchDir);
        generateBatch(codesToModel,batchSize, batchDir);
    }


    @SneakyThrows
    private void generateBatch(Model codesToModel, int batchSize, File batchDir) {
        // LOAD CSVs
        log.info("load csvs...");
        CsvDto csvEnterprises = CSVReaderUtils.readObj(entrepriseCsv.getInputStream());
        CsvDto csvContacts = CSVReaderUtils.readObj(contactCsv.getInputStream());
        CsvDto csvDdenominations = CSVReaderUtils.readObj(denominationCsv.getInputStream());
        CsvDto csvAddresses = CSVReaderUtils.readObj(addressCsv.getInputStream());

        log.info("load csvs done");

        // LOAD enriched data
        log.info("load enriched data...");
        Map<String, List<Map<String, String>>> contactsGroupedByEnterpriseNumber = csvContacts.getCsv()
                .stream().collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));
        Map<String, List<Map<String, String>>> denominationsGroupedByEnterpriseNumber = csvDdenominations.getCsv().stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));
        Map<String, List<Map<String, String>>> addressesGroupedByEnterpriseNumber = csvAddresses.getCsv().stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "")));
        log.info("load enriched data done");


        // RUN batches
        getBatches(csvEnterprises.getCsv(), batchSize).forEach(batch -> {
            try {
                long timestamp = System.currentTimeMillis();
                log.info("running batch {}.ttl", timestamp);
                log.info("load enterprise model...");
                Model model = enterprisesToModel(batch, codesToModel);
                Set<String> enterpriseNumbers = batch.stream()
                        .map(map -> map.get("EnterpriseNumber").replaceAll("\\.", ""))
                        .collect(Collectors.toSet());
                log.info("enrich model with contacts...");

                List<Map<String, String>> contactsForEnterprises = enterpriseNumbers.stream()
                        .map(contactsGroupedByEnterpriseNumber::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(toList());


                enrichModelWithContacts(model, contactsForEnterprises, codesToModel);
                log.info("enrich model with denominations...");

                List<Map<String, String>> denominationsForEnterprises = enterpriseNumbers.stream()
                        .map(denominationsGroupedByEnterpriseNumber::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

                enrichModelWithDenominations(model, denominationsForEnterprises, codesToModel);

                log.info("enrich model with addresses...");

                List<Map<String, String>> adressesForEnterprises = enterpriseNumbers.stream()
                        .map(addressesGroupedByEnterpriseNumber::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

                enrichModelWithAddress(model, adressesForEnterprises, codesToModel);

                RDFDataMgr.write(new FileOutputStream(new File(batchDir, "%s.ttl".formatted(timestamp))), model, RDFFormat.TURTLE);
                Thread.sleep(100);
            } catch (Exception e) {
                log.error("error", e);
            }


        });

    }

    private void enrichModelWithDenominations(Model model, List<Map<String, String>> denominationsForEnterprises, Model codesToModel) {
        Map<String, List<Map<String, String>>> groupedResources = denominationsForEnterprises.stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "").toUpperCase()));

        groupedResources.forEach((enterpriseNumber, value) -> {
            String uri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "denomination", enterpriseNumber);
            org.apache.jena.rdf.model.Resource resource = model.createResource(uri);
            resource.addProperty(MU_UUID, ResourceFactory.createStringLiteral("DEN" + enterpriseNumber.toUpperCase()));
            value.stream().map(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", enterpriseNumber);
                org.apache.jena.rdf.model.Resource js = model.getResource(organizationUri);
                model.add(resource,  ResourceFactory.createProperty(NAMESPACE_PREFIX + "/denominationBelongsTo"), js);
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
                        resource.addLiteral(FOAF.name, ResourceFactory.createLangLiteral(entry.getKey(),entry.getValue()));
                    });
            model.add(resource, RDF.type, DENOMINATION_TYPE);

        });
    }
    private void enrichModelWithAddress(Model model, List<Map<String, String>> addressesForEnterprises, Model codesToModel) {
        Map<String, List<Map<String, String>>> groupedResources = addressesForEnterprises.stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "").toUpperCase()));
        groupedResources.forEach((enterpriseNumber, value) -> {
            String uri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "address", enterpriseNumber);
            org.apache.jena.rdf.model.Resource resource = model.createResource(uri);
            resource.addProperty(MU_UUID, ResourceFactory.createStringLiteral("ADDR" + enterpriseNumber.toUpperCase()));
            value.forEach(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", enterpriseNumber);
                org.apache.jena.rdf.model.Resource org = model.getResource(organizationUri);
                model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/addressBelongsTo"), org);
                ofNullable(v.get("TypeOfAddress")).filter(StringUtils::isNotEmpty).map(toa -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"code", ("TypeOfAddress" + toa).toUpperCase()))
                        .ifPresentOrElse(toaUri -> {
                            org.apache.jena.rdf.model.Resource toa = codesToModel.getResource(toaUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasAddressType"), toa);
                        }, () -> log.trace("'type of address' not found"));

                String streetFr = ofNullable(v.get("StreetFR")).orElse("");
                String municipalityFr = ofNullable(v.get("MunicipalityFR")).orElse("");
                String streetNl = ofNullable(v.get("StreetNL")).orElse("");
                String municipalityNl = ofNullable(v.get("MunicipalityNL")).orElse("");
                String houseNumber = ofNullable(v.get("HouseNumber")).orElse("");
                String zipcode = ofNullable(v.get("Zipcode")).orElse("");

                if(StringUtils.isNotEmpty(streetFr)){
                    String addressFr= "%s %s, %s %s".formatted(streetFr, houseNumber, zipcode, municipalityFr);
                    resource.addProperty(VCARD.ADR, ResourceFactory.createLangLiteral(addressFr, "fr"));
                }
                if(StringUtils.isNotEmpty(streetNl)){
                    String addressNl= "%s %s, %s %s".formatted(streetNl, houseNumber, zipcode, municipalityNl);
                    resource.addProperty(VCARD.ADR, ResourceFactory.createLangLiteral(addressNl, "nl"));
                }


            });


            model.add(resource, RDF.type, ADDR_TYPE);
        });

    }

    private void enrichModelWithContacts(Model model, List<Map<String, String>> contactsForEnterprises, Model codesToModel) {
        Map<String, List<Map<String, String>>> groupedResources = contactsForEnterprises.stream().
                collect(Collectors.groupingBy(map -> map.get("EntityNumber").replaceAll("\\.", "").toUpperCase()));
        groupedResources.forEach((enterpriseNumber, value) -> {
            String uri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "contact", enterpriseNumber);
            org.apache.jena.rdf.model.Resource resource = model.createResource(uri);
            resource.addProperty(MU_UUID, ResourceFactory.createStringLiteral("CTC" + enterpriseNumber.toUpperCase()));

            value.forEach(v -> {
                var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", enterpriseNumber);
                org.apache.jena.rdf.model.Resource js = model.getResource(organizationUri);
                model.add(resource,  ResourceFactory.createProperty(NAMESPACE_PREFIX + "/contactBelongsTo"), js);

                ofNullable(v.get("ContactType")).filter(StringUtils::isNotEmpty)
                        .ifPresentOrElse(contactType -> {
                            if ("EMAIL".equals(contactType)) {
                                resource.addLiteral(FOAF.mbox, ResourceFactory.createStringLiteral(v.get("Value")));
                            } else if ("WEB".equals(contactType)) {
                                resource.addLiteral(FOAF.homepage, ResourceFactory.createStringLiteral(v.get("Value")));
                            } else if ("TEL".equals(contactType)) {
                                resource.addLiteral(FOAF.phone, ResourceFactory.createStringLiteral(v.get("Value")));
                            }

                        }, () -> log.trace("'contact type' not found"));

            });


            model.add(resource, RDF.type, CONTACT_TYPE);
        });

    }

    @SneakyThrows
    private Model codesToModel(File batchDir) {
        long timestamp = System.currentTimeMillis();
        Model model = ModelFactory.createDefaultModel();

        List<Map<String, String>> csvCodes = CSVReaderUtils.readMap(codeCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvCodes.stream().
                collect(Collectors.groupingBy(map -> "%s%s".formatted(map.get("Category"), map.get("Code"))));

        groupedResources.forEach((key, value) -> {
            String categoryCode = key.toUpperCase();

            org.apache.jena.rdf.model.Resource resource = model.createResource("%s/%s/%s".formatted(NAMESPACE_PREFIX, "code",categoryCode));
            resource.addProperty(MU_UUID, ResourceFactory.createStringLiteral(categoryCode));
            value.forEach(lang -> resource.addLiteral(RDFS.label, ResourceFactory.createLangLiteral(lang.get("Description"),lang.get("Language").toLowerCase())));
            model.add(resource, RDF.type, CODE_TYPE);
        });
        RDFDataMgr.write(new FileOutputStream(new File(batchDir, "%s-code.ttl".formatted(timestamp))), model, RDFFormat.TURTLE);
        Thread.sleep(110);
        return model;

    }
    private Model enterprisesToModel(List<Map<String, String>> batch, Model codesToModel) {
        Model model = ModelFactory.createDefaultModel();
        Map<String, List<Map<String, String>>> groupedResources = batch.stream().collect(Collectors.groupingBy(map -> map.get("EnterpriseNumber").replaceAll("\\.", "")));


        groupedResources.forEach((key, value) -> {
            String uri = "%s/%s/%s".formatted(NAMESPACE_PREFIX, "company", key.toUpperCase());
            org.apache.jena.rdf.model.Resource resource = model.createResource(uri);
            resource.addProperty(MU_UUID, ResourceFactory.createStringLiteral(key.toUpperCase()));
            value.forEach(line -> {
                ofNullable(line.get("Status")).filter(StringUtils::isNotEmpty).map(statusCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"code", ("Status" + statusCode).toUpperCase()))
                        .ifPresentOrElse(statusUri -> {
                            org.apache.jena.rdf.model.Resource js = codesToModel.getResource(statusUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStatus"), js);
                        }, () -> log.trace("'status' not found"));

                ofNullable(line.get("JuridicalSituation"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jsCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, "code", ("JuridicalSituation"+jsCode).toUpperCase()))
                        .ifPresentOrElse(juridicalSituationUri -> {
                            org.apache.jena.rdf.model.Resource js = codesToModel.getResource(juridicalSituationUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalSituation"), js);
                        }, () -> log.trace("'juridicalSituationUri' not found"));

                ofNullable(line.get("TypeOfEnterprise"))
                        .filter(StringUtils::isNotEmpty)
                        .map(typeOfEntrepriseCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"code", ("TypeOfEnterprise"+typeOfEntrepriseCode).toUpperCase()))
                        .ifPresentOrElse(typeOfEntrepriseUri -> {
                            org.apache.jena.rdf.model.Resource js = codesToModel.getResource(typeOfEntrepriseUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasTypeOfCompany"), js);
                        }, () -> log.trace("'typeOfEntrepriseUri' not found"));


                ofNullable(line.get("JuridicalForm"))
                        .filter(StringUtils::isNotEmpty)
                        .map(jfCode -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"code", ("JuridicalForm"+jfCode).toUpperCase()))
                        .ifPresentOrElse(juridicalFormUri -> {
                            org.apache.jena.rdf.model.Resource js = codesToModel.getResource(juridicalFormUri);
                            model.add(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasJuridicalForm"), js);
                        }, () -> log.trace("'juridicalFormUri' not found"));
                resource.addLiteral(ResourceFactory.createProperty(NAMESPACE_PREFIX + "/hasStartDate"), ResourceFactory.createStringLiteral(line.get("StartDate")));

            });
            model.add(resource, RDF.type, ORG.Organization);
        });
        return model;
    }


    @Override
    public void run(String... args) throws Exception {
        File batchDir = new File("/tmp/bce_ttl_batch");
        if (batchDir.exists()) {
            FileUtils.deleteDirectory(batchDir);
        }
        batchDir.mkdir();

        log.info("start batch...");
        this.transform(chunkSize, batchDir);
        log.info("batch done.");
        System.exit(0);

    }
}
