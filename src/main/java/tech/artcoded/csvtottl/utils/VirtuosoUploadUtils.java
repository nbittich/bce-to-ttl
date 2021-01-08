package tech.artcoded.csvtottl.utils;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public interface VirtuosoUploadUtils {
    Logger log = LoggerFactory.getLogger(VirtuosoUploadUtils.class);
    static void upload(Model model, String graphUri, String host, String username, String password) {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });

        try {
            StringWriter writer = new StringWriter();
            model.write(writer, "TURTLE");

            String sparqlUrl = host + "/sparql" + "-graph-crud-auth?graph-uri=" + graphUri;
            loadIntoGraph_exception(writer.toString().getBytes(StandardCharsets.UTF_8), sparqlUrl);
        }
        catch (Exception e) {
            log.error("error during upload",e);
        }
    }
    private static void loadIntoGraph_exception(byte[] data, String updateUrl) throws Exception {
        URL url = new URL(updateUrl);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-turtle");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Content-Length", Integer.toString(data.length));
        conn.setUseCaches(false);
        conn.getOutputStream().write(data);
        log.trace("code: {}, message: {}", conn.getResponseCode(),conn.getResponseMessage());
    }
}
