package tech.artcoded.csvtottl.transformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Service
@Slf4j
public class CsvToModelTransformer implements CommandLineRunner {
    @Value("classpath:code.csv")
    private Resource codeCsv;
    @Value("classpath:enterprise.csv")
    private Resource entrepriseCsv;
    @Value("classpath:denomination.csv")
    private Resource denominationCsv;

    private static final String NAMESPACE_PREFIX = "http://bittich.be/bce";
    private static final org.apache.jena.rdf.model.Resource CODE_TYPE = ResourceFactory.createResource(NAMESPACE_PREFIX + "/code");

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

        //Model model = ModelFactory.createDefaultModel();

        List<Map<String, String>> csvDdenominations = CSVReaderUtils.readMap(denominationCsv.getInputStream())
                .stream().filter(entry -> "001".equals(entry.get("TypeOfDenomination"))).collect(Collectors.toList());
        Map<String, List<Map<String, String>>> groupedResources = csvDdenominations.stream().
                collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"Denomination", map.get("EntityNumber").replaceAll("\\.",""))));
        groupedResources.forEach((key, value) -> {
            org.apache.jena.rdf.model.Resource resource = model.createResource(key);
            value.stream().map(v-> {
               var organizationUri = "%s/%s/%s".formatted(NAMESPACE_PREFIX,"Organization", v.get("EntityNumber").replaceAll("\\.",""));
                DataTransformer.addResource(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/org"), organizationUri, List.of(ResourceFactory.createResource(NAMESPACE_PREFIX + "/enterprise")), false);
                String lang = switch (v.get("Language")){
                    case "2" -> "nl";
                    case "3" -> "de";
                    case "4" -> "en";
                    default -> "fr";
                };
                return Map.entry(v.get("Denomination"), lang);
            }).forEach(entry -> {
                DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/denomination"), entry.getValue(), entry.getKey(), false);
                model.add(resource, RDF.type, CODE_TYPE);
            });
            model.add(resource, RDF.type, ResourceFactory.createResource(NAMESPACE_PREFIX + "/denomination"));
        });
       // log.info("save denomination turtle file... it takes a while :'(");

       // model.write(new FileWriter(new File("/tmp", "denominations.ttl")), "TURTLE");
        //log.info("denomination turtle file saved. Thank God!");
    }


    @SneakyThrows
    private void transformEntreprises(Model model) {
        log.info("create enterprise turtle file...");
       // Model model = ModelFactory.createDefaultModel();

        List<Map<String, String>> csvEnterprises = CSVReaderUtils.readMap(entrepriseCsv.getInputStream());


        Map<String, List<Map<String, String>>> groupedResources = csvEnterprises.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX,"Organization", map.get("EnterpriseNumber").replaceAll("\\.",""))));


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
       // log.info("save enterprise turtle file...");

        //model.write(new FileWriter(new File("/tmp", "enterprises.ttl")), "TURTLE");
        //log.info("enterprise turtle file saved.");

    }

    @SneakyThrows
    private void transformCodes(Model model){
        log.info("create codes turtle file...");
       // Model model = ModelFactory.createDefaultModel();

        List<Map<String, String>> csvCodes = CSVReaderUtils.readMap(codeCsv.getInputStream());

        Map<String, List<Map<String, String>>> groupedResources = csvCodes.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(NAMESPACE_PREFIX, map.get("Category"), map.get("Code"))));

         groupedResources.forEach((key, value) -> {
             org.apache.jena.rdf.model.Resource resource = model.createResource(key);
             value.forEach(lang -> DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(NAMESPACE_PREFIX + "/description"), lang.get("Description"), lang.get("Language").toLowerCase(), false));
             model.add(resource, RDF.type, CODE_TYPE);
         });

       // log.info("save codes turtle file...");
       // model.write(new FileWriter(new File("/tmp", "codes.ttl")), "TURTLE");
      //  log.info("codes turtle file saved.");
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("start migration...");
        Model model = this.transform();
        model.write(new FileWriter(new File("/tmp", "bce.ttl")), "TURTLE");

        log.info("migration done.");

    }
}
