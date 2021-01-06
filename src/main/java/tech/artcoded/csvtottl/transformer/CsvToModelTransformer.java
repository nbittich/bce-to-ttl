package tech.artcoded.csvtottl.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import tech.artcoded.csvtottl.utils.CSVReaderUtils;
import tech.artcoded.csvtottl.utils.ModelConverter;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CsvToModelTransformer implements CommandLineRunner {
    @Value("classpath:code.csv")
    private Resource testCsv;

    @SneakyThrows
    public Model transform() {
        String namespacePrefix = "http://bittich.be/bce";
        List<Map<String, String>> codes = CSVReaderUtils.readMap(testCsv.getInputStream());

        Model model = ModelFactory.createDefaultModel();

        Map<String, List<Map<String, String>>> groupedResources = codes.stream().collect(Collectors.groupingBy(map -> "%s/%s/%s".formatted(namespacePrefix, map.get("category"), map.get("code"))));

        groupedResources.forEach((resourceUri, groupedResource) ->{
            org.apache.jena.rdf.model.Resource resource = model.createResource(resourceUri);
            groupedResource.forEach(lang -> DataTransformer.addLangLiteral(resource, ResourceFactory.createProperty(namespacePrefix + "/description"),lang.get("description"),lang.get("language").toLowerCase(), false));
            model.add(resource, RDF.type, ResourceFactory.createResource(namespacePrefix + "/code"));
        });

        return model;
    }

    @Override
    public void run(String... args) throws Exception {
        Model transform = this.transform();
        String turtle = ModelConverter.modelToLang(transform, "TURTLE");
        log.info(turtle);
    }
}
