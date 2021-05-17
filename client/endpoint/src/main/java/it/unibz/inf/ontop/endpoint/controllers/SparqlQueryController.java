package it.unibz.inf.ontop.endpoint.controllers;

import it.unibz.inf.ontop.rdf4j.repository.impl.OntopVirtualRepository;
import it.unibz.inf.ontop.utils.VersionInfo;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLBooleanJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLBooleanXMLWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.query.resultio.text.BooleanTextWriter;
import org.eclipse.rdf4j.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.jsonld.JSONLDWriter;
import org.eclipse.rdf4j.rio.rdfjson.RDFJSONWriter;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLWriter;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED_VALUE;

@RestController
public class SparqlQueryController {

    private static final Logger log = LoggerFactory.getLogger(SparqlQueryController.class);

    private final OntopVirtualRepository repository;

    @Autowired
    public SparqlQueryController(OntopVirtualRepository repository) {
        this.repository = repository;
    }

    @GetMapping(value = "/")
    public ModelAndView home() {
        Map<String, String> model = new HashMap<>();
        model.put("version", VersionInfo.getVersionInfo().getVersion());
        return new ModelAndView("index", model);
    }

    @RequestMapping(value = "/sparql",
            method = {RequestMethod.GET}
    )
    public void query_get(
            @RequestHeader(ACCEPT) String accept,
            @RequestParam(value = "query") String query,
            @RequestParam(value = "default-graph-uri", required = false) String[] defaultGraphUri,
            @RequestParam(value = "named-graph-uri", required = false) String[] namedGraphUri,
            @RequestParam(value = "streaming-mode", required = false) String streamingMode,
            @RequestParam(value = "json", required = false) String JSONmode,
            HttpServletResponse response) {
        execQuery(accept, query, defaultGraphUri, namedGraphUri, streamingMode, JSONmode, response);
    }

    @RequestMapping(value = "/sparql",
            method = RequestMethod.POST,
            consumes = APPLICATION_FORM_URLENCODED_VALUE)
    public void query_post_URL_encoded(
            @RequestHeader(ACCEPT) String accept,
            @RequestParam(value = "query") String query,
            @RequestParam(value = "default-graph-uri", required = false) String[] defaultGraphUri,
            @RequestParam(value = "named-graph-uri", required = false) String[] namedGraphUri,
            @RequestParam(value = "streaming-mode", required = false) String streamingMode,
            @RequestParam(value = "json", required = false) String JSONmode,
            HttpServletResponse response) {
        execQuery(accept, query, defaultGraphUri, namedGraphUri, streamingMode, JSONmode, response);
    }

    @RequestMapping(value = "/sparql",
            method = RequestMethod.POST,
            consumes = "application/sparql-query")
    public void query_post_directly(
            @RequestHeader(ACCEPT) String accept,
            @RequestBody String query,
            @RequestParam(value = "default-graph-uri", required = false) String[] defaultGraphUri,
            @RequestParam(value = "named-graph-uri", required = false) String[] namedGraphUri,
            @RequestParam(value = "streaming-mode", required = false) String streamingMode,
            @RequestParam(value = "json", required = false) String JSONmode,
            HttpServletResponse response) {
        execQuery(accept, query, defaultGraphUri, namedGraphUri, streamingMode, JSONmode, response);
    }

    private void execQuery(String accept, String query, String[] defaultGraphUri, String[] namedGraphUri,
                           String streamingMode, String JSONmode, HttpServletResponse response) {
        try (RepositoryConnection connection = repository.getConnection()) {
            Query q = connection.prepareQuery(QueryLanguage.SPARQL, query);
            OutputStream bao = response.getOutputStream();
            streamingMode = (streamingMode == null) ? "unbounded-buffer" : streamingMode;
            JSONmode = (JSONmode == null) ? "json" : JSONmode;

            if (q instanceof TupleQuery) {
                TupleQuery selectQuery = (TupleQuery) q;
                response.setCharacterEncoding("UTF-8");

                if ("*/*".equals(accept) || accept.contains("jsonl") || (accept.contains("json") && JSONmode.toLowerCase().equals("jsonl"))) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/sparql-results+jsonl;charset=UTF-8");
                    evaluateJSONLSelectQuery(selectQuery, bao, response);
                } else if (accept.contains("json") || (accept.contains("json") && JSONmode.toLowerCase().equals("json"))) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/sparql-results+json;charset=UTF-8");
                    evaluateJSONSelectQuery(selectQuery, new SPARQLResultsJSONWriter(bao), bao, response, streamingMode);
                } else if (accept.contains("xml")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/sparql-results+xml;charset=UTF-8");
                    evaluateSelectQuery(selectQuery, new SPARQLResultsXMLWriter(bao), response, streamingMode);
                } else if (accept.contains("csv")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "text/sparql-results+csv;charset=UTF-8");
                    evaluateSelectQuery(selectQuery, new SPARQLResultsCSVWriter(bao), response, streamingMode);
                } else if (accept.contains("tsv") || accept.contains("text/tab-separated-values")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "text/sparql-results+tsv;charset=UTF-8");
                    evaluateSelectQuery(selectQuery, new SPARQLResultsTSVWriter(bao), response, streamingMode);
                } else {
                    response.setStatus(HttpStatus.NOT_ACCEPTABLE.value());
                }

            } else if (q instanceof BooleanQuery) {
                BooleanQuery askQuery = (BooleanQuery) q;
                boolean b = askQuery.evaluate();

                if ("*/*".equals(accept) || accept.contains("json")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/sparql-results+json");
                    addCacheHeaders(response);
                    BooleanQueryResultWriter writer = new SPARQLBooleanJSONWriter(bao);
                    writer.handleBoolean(b);
                } else if (accept.contains("xml")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/sparql-results+xml");
                    addCacheHeaders(response);
                    BooleanQueryResultWriter writer = new SPARQLBooleanXMLWriter(bao);
                    writer.handleBoolean(b);
                } else if (accept.contains("text")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "text/boolean");
                    addCacheHeaders(response);
                    BooleanQueryResultWriter writer = new BooleanTextWriter(bao);
                    writer.handleBoolean(b);
                } else {
                    response.setStatus(HttpStatus.NOT_ACCEPTABLE.value());
                }
            } else if (q instanceof GraphQuery) {
                GraphQuery graphQuery = (GraphQuery) q;
                response.setCharacterEncoding("UTF-8");

                if ("*/*".equals(accept) || accept.contains("turtle")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "text/turtle;charset=UTF-8");
                    evaluateGraphQuery(graphQuery, new TurtleWriter(bao), response);
                } else if (accept.contains("rdf+json")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/rdf+json;charset=UTF-8");
                    evaluateGraphQuery(graphQuery, new RDFJSONWriter(bao, RDFFormat.RDFJSON), response);
                } else if (accept.contains("json")) {
                    // specification of rdf/json, recommend the use of json-ld (we use it as default)
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/ld+json;charset=UTF-8");
                    evaluateGraphQuery(graphQuery, new JSONLDWriter(bao), response);
                }
                else if (accept.contains("xml")) {
                    response.setHeader(HttpHeaders.CONTENT_TYPE, "application/rdf+xml;charset=UTF-8");
                    evaluateGraphQuery(graphQuery, new RDFXMLWriter(bao), response);
                } else {
                    response.setStatus(HttpStatus.NOT_ACCEPTABLE.value());
                }
            } else if (q instanceof Update) {
                response.setStatus(HttpStatus.NOT_IMPLEMENTED.value());
            } else {
                response.setStatus(HttpStatus.BAD_REQUEST.value());
            }
            bao.flush();
        } catch (QueryResultHandlerException e){
            System.out.println("CLIENT CONNECTION CLOSED");
        } catch (IOException ex) {
            throw new Error(ex);
        }
    }

    private void evaluateSelectQuery(TupleQuery selectQuery, TupleQueryResultWriter writer, HttpServletResponse response, String streamingMode) {
        addCacheHeaders(response);
        TupleQueryResult result = selectQuery.evaluate();

        switch (streamingMode.toLowerCase()) {
            case "single-element":
                individualResponse(result, writer, response);
                break;
            case "unbounded-buffer":
                unboundedBufferResponse(result, writer, response);
                break;
        }
    }

    private void evaluateJSONSelectQuery(TupleQuery selectQuery, TupleQueryResultWriter writer, OutputStream bao, HttpServletResponse response, String streamingMode) {
        addCacheHeaders(response);
        TupleQueryResult result = selectQuery.evaluate();

        switch (streamingMode.toLowerCase()) {
            case "single-element":
                individualResponse(result, writer, bao, response);
                break;
            case "unbounded-buffer":
                unboundedBufferResponse(result, writer, response);
                break;
        }
    }

    private void individualResponse(TupleQueryResult result, TupleQueryResultWriter writer, HttpServletResponse response) throws QueryResultHandlerException{
        while (result.hasNext()) {
            synchronized (response) {
                writer.startQueryResult(result.getBindingNames());
                writer.handleSolution(result.next());
                writer.endQueryResult();
                try {
                    response.flushBuffer();
                } catch (IOException e) {
                    throw new QueryResultHandlerException("");
                }
            }
        }
    }

    /*
     *  JSON output encoding. Adds an endline at the end of the message.
     */
    private void individualResponse(TupleQueryResult result, TupleQueryResultWriter writer, OutputStream bao, HttpServletResponse response) throws QueryResultHandlerException{
        while (result.hasNext()) {
            synchronized (response) {
                writer.startQueryResult(result.getBindingNames());
                writer.handleSolution(result.next());
                writer.endQueryResult();
                try {
                    response.flushBuffer();
                    bao.write('\n');
                } catch (IOException e) {
                    throw new QueryResultHandlerException("");
                }
            }
        }
    }

    /*
     *  JSON Lines output encoding, according to the W3C guidelines (https://www.w3.org/TR/2013/REC-sparql11-results-json-20130321/)
     */
    private void evaluateJSONLSelectQuery(TupleQuery selectQuery, OutputStream bao, HttpServletResponse response) throws QueryResultHandlerException{
        addCacheHeaders(response);
        TupleQueryResult result = selectQuery.evaluate();

        while (result.hasNext()) {
            BindingSet binding = result.next();

            String res = "";
            res = res + "{\"head\" : {\"vars\" : [";

            Iterator<String> it = binding.getBindingNames().iterator();
            res = res + "\"" + it.next() + "\"";
            while(it.hasNext()){
                res = res + ", \"" + it.next() + "\"";
            }

            res = res + "]}, \"results\" : {\"bindings\" : [{";

            Iterator<Binding> bindingIter = binding.iterator();
            while (bindingIter.hasNext()) {
                Binding binding2 = bindingIter.next();
                res = res + "\"" + binding2.getName() + "\" : ";
                try {
                    res = res + writeJSONLValue(binding2.getValue());
                } catch (IOException e) {
                    throw new QueryResultHandlerException("");
                }
                if (bindingIter.hasNext()){
                    res = res + ", ";
                }
            }

            res = res + "}]}}\n";
            byte[] byteArrray = res.getBytes(StandardCharsets.UTF_8);

            try {
                bao.write(byteArrray);
            } catch (IOException e) {
                throw new QueryResultHandlerException("");
            }
        }
    }

    private String writeJSONLValue(Value value) throws IOException, QueryResultHandlerException {
        String result = "{";

        if (value instanceof IRI) {
            result = result + "\"type\" : \"uri\"";
            result = result + ", \"value\" : " + "\"" + ((IRI) value).toString() + "\"";
        } else if (value instanceof BNode) {
            result = result + "\"type\" : \"bnode\",";
            result = result + ", \"value\" : " + "\"" + ((BNode) value).getID() + "\"";
        } else if (value instanceof Literal) {
            Literal lit = (Literal) value;
            result = result + "\"type\" : \"literal\"";
            result = result + ", \"value\" : " + "\"" + lit.getLabel() + "\"";

            if (Literals.isLanguageLiteral(lit)) {
                result = result + ", \"xml:lang\" : " + "\"" + lit.getLanguage().orElse(null) + "\"";
            } else {
                IRI datatype = lit.getDatatype();
                boolean ignoreDatatype = datatype.equals(XSD.STRING);
                if (!ignoreDatatype) {
                    result = result + ", \"datatype\" : " + "\"" + lit.getDatatype().stringValue() + "\"";
                }
            }
        } else {
            throw new TupleQueryResultHandlerException("Unknown Value object type: " + value.getClass());
        }

        result = result + "}";
        return result;
    }


    private void unboundedBufferResponse(TupleQueryResult result, TupleQueryResultWriter writer, HttpServletResponse response) throws QueryResultHandlerException{
        writer.startQueryResult(result.getBindingNames());
        while (result.hasNext()) {
            synchronized (response){
                writer.handleSolution(result.next());
                try {
                    response.flushBuffer();
                } catch (IOException e) {
                    throw new QueryResultHandlerException("");
                }
            }
        }
        writer.endQueryResult();
    }

    private void evaluateGraphQuery(GraphQuery graphQuery, RDFWriter turtleWriter, HttpServletResponse response) {
        addCacheHeaders(response);
        graphQuery.evaluate(turtleWriter);
    }

    /**
     * TODO: try to find a way to detect if the query is cacheable or not
     * (e.g. not including non-deterministic functions like NOW())
     */
    private void addCacheHeaders(HttpServletResponse response) {
        repository.getHttpCacheHeaders().getMap()
                .forEach(response::setHeader);
    }

    @ExceptionHandler({MalformedQueryException.class})
    public ResponseEntity<String> handleMalformedQueryException(Exception ex) {
        ex.printStackTrace();
        String message = ex.getMessage();
        HttpHeaders headers = new HttpHeaders();
        headers.set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        HttpStatus status = HttpStatus.BAD_REQUEST;
        return new ResponseEntity<>(message, headers, status);
    }

    @ExceptionHandler({RepositoryException.class, Exception.class})
    public ResponseEntity<String> handleRepositoryException(Exception ex) {
        ex.printStackTrace();
        String message = ex.getMessage();
        HttpHeaders headers = new HttpHeaders();
        headers.set(CONTENT_TYPE, "text/plain; charset=UTF-8");
        HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
        return new ResponseEntity<>(message, headers, status);
    }

}
