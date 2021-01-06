package tech.artcoded.csvtottl.transformer;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public interface DataTransformer {
    Logger log = LoggerFactory.getLogger(DataTransformer.class);

    static void addResource(Resource resource, Property property, String valueUri, List<Resource> rdfTypes, boolean mandatory) {
        Resource valueResource = StringUtils.isNotBlank(valueUri) ? resource.getModel().createResource(valueUri) : null;
        add(resource, property, valueResource, rdfTypes, mandatory);
    }

    static void addDateValue(Resource resource, Property property, Calendar value, boolean mandatory) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Literal valueResource = value != null ? resource.getModel().createTypedLiteral(dateFormat.format(value.getTime()), XSDDatatype.XSDdate) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    static void addDateValue(Resource resource, Property property, String value, boolean mandatory) {
        Literal valueResource = value != null ? resource.getModel().createTypedLiteral(value, XSDDatatype.XSDdate) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    static void addBooleanValue(Resource resource, Property property, Boolean value, Boolean mandatory) {
        Literal valueResource = value != null ? resource.getModel().createTypedLiteral(value) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    static void addIntegerValue(Resource resource, Property property, Integer value, Boolean mandatory) {
        Literal valueResource = value != null ? resource.getModel().createTypedLiteral(value) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    static void addLiteral(Resource resource, Property property, String value, Boolean mandatory) {
        Literal valueResource = StringUtils.isNotBlank(value) ? resource.getModel().createTypedLiteral(value) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    static void addLangLiteral(Resource resource, Property property, String value, String lang, Boolean mandatory) {
        Literal valueResource = StringUtils.isNotBlank(value) ? resource.getModel().createLiteral(value, lang) : null;
        add(resource, property, valueResource, Collections.emptyList(), mandatory);
    }

    private static void add(Resource resource, Property property, RDFNode value, List<Resource> rdfTypes, boolean mandatory) {
        if (null == value) {
            if (mandatory) {
                log.warn("Mandatory value for '{}'  not found for '{}'. Uri: {}", property, resource, property.getURI());
            }
            return;
        }

        resource.addProperty(property, value);
        if (value.isResource()) {
            if (value.asResource().getModel().contains(null, null, value)) {
                rdfTypes.forEach(type -> resource.getModel().add(value.asResource(), RDF.type, type));
            }
        }
    }


    private static Calendar filterCalendar(Calendar calendar) {
        if (calendar == null) {
            return null;
        } else if (calendar.get(Calendar.YEAR) >= 2099 || calendar.get(Calendar.YEAR) <= 1600) {
            log.warn("Invalid date range: '{}'", calendar.get(Calendar.YEAR));
            return null;
        } else {
            return calendar;
        }
    }

    static Set<Calendar> getDates(String content) {
        if (StringUtils.isBlank(content)) return Collections.emptySet();
        else
            return splitMultiple(content).stream().map(DataTransformer::getCalendarFromDateFormat).collect(Collectors.toSet());
    }


    static List<String> splitMultiple(String cellContent) {
        final String separator = "\";\"";
        final String separatorAlt = "”;”";
        if (StringUtils.isBlank(cellContent)) return Collections.emptyList();
        if (cellContent.contains(separatorAlt)) cellContent = cellContent.replaceAll(separatorAlt, separator);
        return Arrays.stream(cellContent.split(separator)).map(String::trim).collect(Collectors.toList());
    }

    static Calendar getCalendarFromDateFormat(String originalValue) {
        Calendar calendar = Calendar.getInstance();
        Arrays.asList("dd-MM-yyyy", "dd-MM-yy", "dd/MM/yyyy", "dd/MM/yy")
                .forEach(format -> {
                    try {
                        Date date = new SimpleDateFormat(format).parse(originalValue);
                        if (Objects.nonNull(date)) {
                            calendar.setTime(date);
                        }
                    } catch (ParseException ignore) {
                    }
                });
        log.warn(calendar != null ? calendar.get(Calendar.DAY_OF_MONTH) + "/" + (calendar.get(Calendar.MONTH) + 1) + "/" + calendar.get(Calendar.YEAR) : "NULL", originalValue);
        if (null != calendar) return filterCalendar(calendar);
        return null;
    }
}