package tech.artcoded.csvtottl.utils;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SparqlUtil {
    Logger log = LoggerFactory.getLogger(SparqlUtil.class);

    static void load(Model model, String graphUri, String host, String username, String password){
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(username, password);
        credsProvider.setCredentials(AuthScope.ANY, credentials);
        HttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        try (RDFConnection conn = RDFConnectionRemote.create()
                .destination(host)
                .httpClient(httpclient)
                .build()) {
            conn.load(graphUri, model);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
