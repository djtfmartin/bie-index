/*
 * Copyright (C) 2016 Atlas of Living Australia
 * All Rights Reserved.
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */

package au.org.ala.bie

import au.com.bytecode.opencsv.CSVReader
import au.org.ala.bie.indexing.RankedName
import au.org.ala.bie.search.IndexDocType
import au.org.ala.bie.util.Encoder
import au.org.ala.bie.util.TitleCapitaliser
import au.org.ala.names.model.RankType
import au.org.ala.names.model.TaxonomicType
import au.org.ala.vocab.ALATerm
import grails.async.PromiseList
import grails.converters.JSON
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang.StringUtils
import org.apache.solr.client.solrj.util.ClientUtils
import org.apache.solr.common.params.MapSolrParams
import org.gbif.api.vocabulary.TaxonomicStatus
import org.gbif.dwc.terms.*
import org.gbif.dwca.io.Archive
import org.gbif.dwca.io.ArchiveFactory
import org.gbif.dwca.io.ArchiveFile
import org.gbif.dwca.record.Record
import org.gbif.dwca.record.StarRecord
import org.grails.web.json.JSONElement
import org.grails.web.json.JSONObject
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import org.springframework.web.util.UriUtils

import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream

/**
 * Services for data importing.
 */
class ImportService {
    static IN_SCHEMA = [
            DwcTerm.taxonID, DwcTerm.nomenclaturalCode, DwcTerm.establishmentMeans, DwcTerm.taxonomicStatus,
            DwcTerm.taxonConceptID, DwcTerm.scientificNameID, DwcTerm.nomenclaturalStatus, DwcTerm.nameAccordingTo, DwcTerm.nameAccordingToID,
            DwcTerm.scientificNameID, DwcTerm.namePublishedIn, DwcTerm.namePublishedInID, DwcTerm.namePublishedInYear,
            DwcTerm.taxonRemarks, DwcTerm.lifeStage, DwcTerm.sex, DwcTerm.locationID, DwcTerm.locality, DwcTerm.countryCode,
            DcTerm.source, DcTerm.language, DcTerm.license, DcTerm.format, DcTerm.rights, DcTerm.rightsHolder, DcTerm.temporal,
            ALATerm.status, ALATerm.nameID, ALATerm.nameFormatted, ALATerm.nameComplete, ALATerm.priority,
            ALATerm.verbatimNomenclaturalCode, ALATerm.verbatimNomenclaturalStatus, ALATerm.verbatimTaxonomicStatus,
            DwcTerm.datasetName, DcTerm.provenance,
            GbifTerm.isPlural, GbifTerm.isPreferredName, GbifTerm.organismPart, ALATerm.labels
    ]
    // Terms that have been algorithmically added so needn't be added as extras
    static TAXON_ALREADY_INDEXED = [
            DwcTerm.taxonID,
            DwcTerm.datasetID,
            DwcTerm.acceptedNameUsageID,
            DwcTerm.parentNameUsageID,
            DwcTerm.scientificName,
            DwcTerm.taxonRank,
            DwcTerm.scientificNameAuthorship,
            DwcTerm.taxonomicStatus,
            ALATerm.nameComplete,
            ALATerm.nameFormatted,
            DwcTerm.taxonRemarks,
            DcTerm.provenance
    ] as Set
    // Count report interval
    static REPORT_INTERVAL = 100000
    // Batch size for solr queries/commits and page sizes
    static BATCH_SIZE = 5000
    // Buffer size for commits
    static BUFFER_SIZE = 1000


    def indexService, searchService

    def grailsApplication
    def speciesGroupService
    def conservationListsSource
    def jobService

    def brokerMessagingTemplate

    def static DYNAMIC_FIELD_EXTENSION = "_s"
    def static IMAGE_FIELDS = "taxon_concept_lsid, kingdom, phylum, class, order, family, genus, species, taxon_name, image_url, data_resource_uid"
    def isKeepIndexing = true // so we can cancel indexing thread (single thread only so field is OK)

    static {
        TermFactory tf = TermFactory.instance()
        for (Term term: ALATerm.values())
            tf.addTerm(term.qualifiedName(), term)
    }

    /**
     * Retrieve a set of file paths from the import directory.
     */
    def retrieveAvailableDwCAPaths() {

        def filePaths = []
        def importDir = new File(grailsApplication.config.getProperty('import.taxonomy.dir'))
        if (importDir.exists()) {
            File[] expandedDwc = importDir.listFiles()
            expandedDwc.each {
                if (it.isDirectory()) {
                    filePaths << it.getAbsolutePath()
                }
            }
        }
        filePaths
    }

    def importAll() {
        log "Starting import of all data"
        String[] sequence = grailsApplication.config.getProperty('import.sequence').split(',')
        for (String step: sequence) {
            if (!jobService.current || jobService.current.cancelled) {
                log "Cancelled"
                return
            }
            step = step.trim().toLowerCase()
            log("Step ${step}")
            try {
                switch (step) {
                    case 'collectory':
                        importCollectory()
                        break
                    case 'conservation-lists':
                        importConservationSpeciesLists()
                        break
                    case 'denormalise':
                    case 'denormalize':
                        denormaliseTaxa(false)
                        break
                    case 'images':
                        loadImages(false)
                        break
                    case 'layers':
                        importLayers()
                        break
                    case 'link-identifiers':
                        buildLinkIdentifiers(false)
                        break
                    case 'localities':
                        importLocalities()
                        break
                    case 'occurrences':
                        importOccurrenceData()
                        break
                    case 'regions':
                        importRegions()
                        break
                    case 'taxonomy-all':
                        importAllDwcA()
                        break
                    case 'vernacular':
                        importVernacularSpeciesLists()
                        break
                    case 'wordpress':
                        importWordPressPages()
                        break
                    default:
                        log("Unknown step ${step}")
                        log.error("Unknown step ${step}")
                }
            } catch (Exception ex) {
                def message = "Problem in step ${step}: ${ex.getMessage()}"
                log(message)
                log.error(message, ex)
            }
        }
         log "Finished import of all data"
    }

    def importAllDwcA() {
        log "Starting import of all taxon field"
        clearTaxaIndex()
        def filePaths = retrieveAvailableDwCAPaths()
        filePaths.each {
            importDwcA(it, false)
        }
        log "Finished import of all taxon field"
    }

    /**
     * Import layer information into the index.
     *
     * @return
     */
    def importLayers() {
        log "Starting layer import"
        def js = new JsonSlurper()
        def url = grailsApplication.config.layersServicesUrl + "/layers"
        log "Requesting layer list from : ${url}"
        def layers = js.parseText(new URL(Encoder.encodeUrl(url)).getText("UTF-8"))
        def batch = []
        indexService.deleteFromIndex(IndexDocType.LAYER)
        layers.each { layer ->
            def doc = [:]
            doc["id"] = layer.name
            doc["guid"] = layer.name
            doc["idxtype"] = IndexDocType.LAYER.name()
            doc["name"] = layer.displayname
            doc["description"] = layer.description
            doc["distribution"] = "N/A"
            log "Importing layer ${layer.displayname}"
            batch << doc
        }
        indexService.indexBatch(batch)
        log"Finished indexing ${layers.size()} layers"
        log "Finsihed layer import"
    }

    def importLocalities() {
        log "Starting localities import"
        if(grailsApplication.config.gazetteerLayerId) {
            indexService.deleteFromIndex(IndexDocType.LOCALITY)
            log("Starting indexing ${grailsApplication.config.gazetteerLayerId}")
            def metadataUrl = grailsApplication.config.layersServicesUrl + "/layer/" + grailsApplication.config.gazetteerLayerId + "?enabledOnly=false"
            log("Getting metadata for layer: ${metadataUrl}")
            def js = new JsonSlurper()
            def layer = js.parseText(new URL(Encoder.encodeUrl(metadataUrl)).getText("UTF-8"))
            log("Starting indexing ${layer.id} - ${layer.name} gazetteer layer")
            importLayer(layer)
            log("Finished indexing ${layer.id} - ${layer.name} gazetteer layer")
        } else {
            log("Skipping localities, no gazetteer layer ID configured")
        }
        log "Finished localities import"
    }

    def importRegions() {
        log "Starting regions import"
        def js = new JsonSlurper()
        def layers = js.parseText(new URL(Encoder.encodeUrl(grailsApplication.config.layersServicesUrl + "/layers")).getText("UTF-8"))
        indexService.deleteFromIndex(IndexDocType.REGION)
        layers.each { layer ->
            if (layer.type == "Contextual") {
                importLayer(layer)
            }
        }
        log"Finished indexing ${layers.size()} region layers"
        log "Finished regions import"

    }

    /**
     * Import layer into index.
     *
     * @param layer
     * @return
     */
    private def importLayer(layer) {
        log("Loading regions from layer " + layer.name)
        def keywords = []

        if (grailsApplication.config.localityKeywordsUrl) {
            keywords = this.getConfigFile(grailsApplication.config.localityKeywordsUrl)
        }

        def tempFilePath = "/tmp/objects_${layer.id}.csv.gz"
        def url = grailsApplication.config.layersServicesUrl + "/objects/csv/cl" + layer.id
        def file = new File(tempFilePath).newOutputStream()
        file << new URL(Encoder.encodeUrl(url)).openStream()
        file.flush()
        file.close()

        if (new File(tempFilePath).exists() && new File(tempFilePath).length() > 0) {

            def gzipInput = new GZIPInputStream(new FileInputStream(tempFilePath))

            //read file and index
            def csvReader = new CSVReader(new InputStreamReader(gzipInput))

            def expectedHeaders = ["pid", "id", "name", "description", "centroid", "featuretype"]

            def headers = csvReader.readNext()
            def currentLine = []
            def batch = []
            while ((currentLine = csvReader.readNext()) != null) {

                if (currentLine.length >= expectedHeaders.size()) {

                    def doc = [:]
                    doc["id"] = currentLine[0]
                    doc["guid"] = currentLine[0]

                    if (currentLine[5] == "POINT") {
                        doc["idxtype"] = IndexDocType.LOCALITY.name()
                    } else {
                        doc["idxtype"] = IndexDocType.REGION.name()
                    }

                    doc["name"] = currentLine[2]

                    if (currentLine[3] && currentLine[2] != currentLine[3]) {
                        doc["description"] = currentLine[3]
                    } else {
                        doc["description"] = layer.displayname
                    }

                    doc["centroid"] = currentLine[4]


                    doc["distribution"] = "N/A"

                    keywords.each {
                        if(doc["description"].contains(it)){
                            doc["distribution"] = it
                        }
                    }

                    batch << doc

                    if (batch.size() > BATCH_INTERVAL) {
                        indexService.indexBatch(batch)
                        batch.clear()
                    }
                }
            }
            if (batch) {
                indexService.indexBatch(batch)
                batch.clear()
            }
        }
    }

    def importHabitats() {

        def batch = []
        indexService.deleteFromIndex(IndexDocType.HABITAT)

        //read the DwC metadata
        Archive archive = ArchiveFactory.openArchive(new File("/data/habitat/"));
        ArchiveFile habitatArchiveFile = archive.getCore()

        Iterator<Record> iter = habitatArchiveFile.iterator()

        //get terms
        Term parentHabitatIDTerm = habitatArchiveFile.getField("http://ala.org.au/terms/1.0/parentHabitatID").getTerm()
        Term habitatNameTerm = habitatArchiveFile.getField("http://ala.org.au/terms/1.0/habitatName").getTerm()

        while (iter.hasNext()) {
            Record record = iter.next()
            def habitatID = record.id()
            def parentHabitatID = record.value(parentHabitatIDTerm)
            def habitatName = record.value(habitatNameTerm)
            def doc = [:]
            if (habitatID) {
                doc["id"] = habitatID
                doc["guid"] = habitatID
                if (parentHabitatID) {
                    doc["parentGuid"] = parentHabitatID
                }
                doc["idxtype"] = IndexDocType.HABITAT.name()
                doc["name"] = habitatName
                batch << doc
            }
        }
        indexService.indexBatch(batch)
    }

    /**
     * Import collectory information into the index.
     *
     * @return
     */
    def importCollectory() {
        log "Starting collectory import"
        [
                "dataResource": IndexDocType.DATARESOURCE,
                "dataProvider": IndexDocType.DATAPROVIDER,
                "institution" : IndexDocType.INSTITUTION,
                "collection"  : IndexDocType.COLLECTION
        ].each { entityType, indexDocType ->
            def js = new JsonSlurper()
            def entities = []
            def drLists = js.parseText(new URL(Encoder.encodeUrl(grailsApplication.config.collectoryServicesUrl + "/${entityType}")).getText("UTF-8"))
            log("About to import ${drLists.size()} ${entityType}")
            log("Clearing existing: ${entityType}")
            indexService.deleteFromIndex(indexDocType)

            drLists.each {
                def details = js.parseText(new URL(Encoder.encodeUrl(it.uri)).getText("UTF-8"))
                def doc = [:]
                doc["id"] = it.uri
                doc["datasetID"] = details.uid
                doc["guid"] = details.alaPublicUrl
                doc["idxtype"] = indexDocType.name()
                doc["name"] = details.name
                doc["description"] = details.pubDescription
                doc["distribution"] = "N/A"

                if (details.rights)
                    doc["rights"] = details.rights
                if (details.licenseType)
                    doc["license"] = (details.licenseType + " " + details.licenseVersion ?: "").trim()
                if (details.acronym)
                    doc["acronym"] = details.acronym

                entities << doc

                if (entities.size() > 10) {
                    indexService.indexBatch(entities)
                    entities.clear()
                }
            }
            log("Cleared")
            if (entities) {
                indexService.indexBatch(entities)
            }
            log("Finished indexing ${drLists.size()} ${entityType}")
        }
        log "Finished collectory import"
    }

    /**
     * Index WordPress pages
     */
    def importWordPressPages() throws Exception {
        log "Starting wordpress import"
        // clear the existing WP index
        indexService.deleteFromIndex(IndexDocType.WORDPRESS)
        if (!grailsApplication.config.wordPress.sitemapUrl) {
            return
        }

        // WordPress variables
        String wordPressSitemapUrl = grailsApplication.config.wordPress.sitemapUrl
        String wordPressBaseUrl = grailsApplication.config.wordPress.baseUrl
        List excludedCategories = grailsApplication.config.wordPress.excludedCategories
        String contentOnlyParams = grailsApplication.config.wordPress.contentOnlyParams
        // get List of WordPress document URLs (each page's URL)
        List docUrls = crawlWordPressSite(wordPressSitemapUrl)
        def documentCount = 0
        def totalDocs = docUrls.size()
        def buffer = []
        log("WordPress pages found: ${totalDocs}") // update user via socket

        // slurp and build each SOLR doc (add to buffer)
        docUrls.each { pageUrl ->
            log "indexing url: ${pageUrl}"
            try {
                // Crawl and extract text from WP pages
                Document document = Jsoup.connect(pageUrl + contentOnlyParams).get();
                String title = document.select("head > title").text();
                String id = document.select("head > meta[name=id]").attr("content");
                String shortlink = document.select("head > link[rel=shortlink]").attr("href");
                String bodyText = document.body().text();
                Elements postCategories = document.select("ul[class=post-categories]");
                List categoriesOut = []
                Boolean excludePost = false;

                if (StringUtils.isEmpty(id) && StringUtils.isNotBlank(shortlink)) {
                    // e.g. http://www.ala.org.au/?p=24241
                    id = StringUtils.split(shortlink, "=")[1];
                }

                if (!postCategories.isEmpty()) {
                    // Is a WP post (not page)
                    Elements categoriesIn = postCategories.select("li > a"); // get list of li elements

                    for (Element cat : categoriesIn) {
                        String thisCat = cat.text();

                        if (thisCat != null && excludedCategories.contains(thisCat)) {
                            // exclude category "button" posts
                            excludePost = true;
                        }
                        if (thisCat != null) {
                            // add category to list
                            categoriesOut.add(thisCat.replaceAll(" ", "_"));
                        }
                    }
                }

                if (excludePost) {
                    log("Excluding post (id: ${id} with category: ${categoriesOut.join('|')}")
                    return
                }

                documentCount++;
                // create SOLR doc
                log(documentCount + ". Indexing WP page - id: " + id + " | title: " + title + " | text: " + StringUtils.substring(bodyText, 0, 100) + "... ");
                def doc = [:]
                doc["idxtype"] = IndexDocType.WORDPRESS.name()

                if (StringUtils.isNotBlank(shortlink)) {
                    doc["guid"] = shortlink
                } else if (StringUtils.isNotEmpty(id)) {
                    doc["guid"] = wordPressBaseUrl + id
                    // use page_id based URI instead of permalink in case permalink is too long for id field
                } else {
                    // fallback
                    doc["guid"] = pageUrl
                }

                doc["id"] = "wp" + id // probably not needed but safer to leave in
                doc["name"] = title // , 1.2f
                doc["content"] = bodyText
                doc["linkIdentifier"] = pageUrl
                //doc["australian_s"] = "recorded" // so they appear in default QF search
                doc["categories"] = categoriesOut
                // add to doc to buffer (List)
                buffer << doc
                // update progress bar (number output only)
                if (documentCount > 0) {
                    updateProgressBar(totalDocs, documentCount)
                }
            } catch (IOException ex) {
                // catch it so we don't stop indexing other pages
                log("Problem accessing/reading WP page <${pageUrl}>: " + ex.getMessage() + " - document skipped")
                log.warn(ex.getMessage(), ex);
            }
        }
        log("Committing to SOLR...")
        indexService.indexBatch(buffer)
        updateProgressBar(100, 100) // complete progress bar
        log "Finished wordpress import"
    }

    /**
     * Read WP sitemap.xml file and return a list of page URLs
     *
     * @param siteMapUrl
     * @return
     */
    private List crawlWordPressSite(String siteMapUrl) throws Exception {

        List pageUrls = []
        // get list of pages to crawl via Google sitemap xml file
        // Note: sitemap.xml files can be nested, so code may need to read multiple files in the future (recursive function needed)
        Document doc = Jsoup.connect(siteMapUrl).get();
        Elements pages = doc.select("loc");
        log.info("Sitemap file lists " + pages.size() + " pages.");

        for (Element page : pages) {
            // add it to list of page urls Field
            pageUrls.add(page.text());
        }

        pageUrls
    }

    /**
     * Removes field values from all records in index
     * @param fld
     * @throws Exception
     */
    def clearFieldValues(String fld) throws Exception {
        int page = 1
        int pageSize = 1000
        def js = new JsonSlurper()
        def baseUrl = grailsApplication.config.indexOfflineBaseUrl

        try {
            while (true) {
                def solrServerUrl = baseUrl + "/select?wt=json&q=*:*&fq=" + fld + ":[*+TO+*]&start=0&rows=" + pageSize //note, always start at 0 since getting rid of all values
                log.info("SOLR clear field URL: " + solrServerUrl)
                def queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                def json = js.parseText(queryResponse)
                int total = json.response.numFound
                def docs = json.response.docs
                def buffer = []

                if (docs.isEmpty())
                    break
                docs.each { doc ->
                    def update = [:]
                    Map<String, String> partialUpdateNull = new HashMap<String, String>();
                    partialUpdateNull.put("set", null);
                    update["id"] = doc.id // doc key
                    update["idxtype"] = ["set": doc.idxtype] // required field
                    update["guid"] = ["set": doc.guid] // required field
                    update[fld] = partialUpdateNull
                    buffer << update
                }
                if (!buffer.isEmpty()) {
                    log.info("Committing cleared fields to SOLR: #" + page.toString() + " set of " + pageSize.toString() + " records")
                    indexService.indexBatch(buffer)
                }
                page++
            }
        } catch (Exception ex) {
            log.error("Unable to clear field " + fld + " values ", ex)
            log("Error: " + ex.getMessage())
        }
    }

    /**
     * Import and index species lists for:
     * <ul>
     *   <li> conservation status </li>
     *   <li> sensitive ? </li>
     *   <li> host-pest interactions ? </li>
     * </ul>
     * For each taxon in each list, update that taxon's SOLR doc with additional fields
     */
    def importConservationSpeciesLists() throws Exception {
        def speciesListUrl = grailsApplication.config.speciesList.url
        def speciesListParams = grailsApplication.config.speciesList.params
        def defaultSourceField = conservationListsSource.defaultSourceField
        def lists = conservationListsSource.lists
        Integer listNum = 0
        def speciesListInfoUrl = grailsApplication.config?.speciesListInfo?.url ?: ''
        String speciesListName = ''
        def deleteFirst = conservationListsSource.deleteFirst

        if (deleteFirst) {
            String[] delFirstFields = deleteFirst.split(',')
            delFirstFields.each { fld ->
                log("Deleting field contents for: " + fld)
                clearFieldValues(fld)
            }
        }

        lists.each { resource ->
            listNum++
            this.updateProgressBar(lists.size(), listNum)
            String uid = resource.uid
            String solrField = resource.field ?: "conservationStatus_s"
            String sourceField = resource.sourceField ?: defaultSourceField
            String action = resource.action ?: "set"
            if (uid && solrField) {
                def url = "${speciesListUrl}${uid}${speciesListParams}"
                log("Loading list from: " + url)
                def urlInfo = "${speciesListInfoUrl}${uid}"
                try {
                    JSONElement json = JSON.parse(getStringForUrl(url))
                    if (speciesListInfoUrl) {
                        log.info("species list info: " + urlInfo)
                        JSONElement jsonInfo = JSON.parse(getStringForUrl(urlInfo))
                        speciesListName = jsonInfo.listName
                    }
                    updateDocsWithConservationStatus(json, sourceField, solrField, uid, action, speciesListName)
                } catch (Exception ex) {
                    def msg = "Error calling webservice: ${ex.message}"
                    log(msg)
                    log.warn(msg, ex) // send to user via http socket
                }
            }
        }
    }

    def importVernacularSpeciesLists() throws Exception {
        def speciesListUrl = grailsApplication.config.speciesList.url
        def speciesListParams = grailsApplication.config.speciesList.params
        def config = this.getConfigFile(grailsApplication.config.vernacularListsUrl)
        def lists = config.lists
        Integer listNum = 0

        lists.each { resource ->
            listNum++
            this.updateProgressBar(lists.size(), listNum)
            String uid = resource.uid
            String vernacularNameField = resource.vernacularNameField ?: config.defaultVernacularNameField
            String nameIdField = resource.nameIdField ?: config.defaultNameIdField
            String statusField = resource.statusField ?: config.defaultStatusField
            String languageField = resource.languageField ?: config.defaultLanguageField
            String sourceField = resource.sourceField ?: config.defaultSourceField
            String resourceLanguage = resource.language ?: config.defaultLanguage
            if (uid && vernacularNameField) {
                def url = "${speciesListUrl}${resource.uid}${speciesListParams}"
                log("Loading list from: " + url)
                try {
                    JSONElement json = JSON.parse(getStringForUrl(url))
                    importAdditionalVernacularNames(json, vernacularNameField, nameIdField, statusField, languageField, sourceField, resourceLanguage, uid)
                } catch (Exception ex) {
                    def msg = "Error calling webservice: ${ex.message}"
                    log(msg)
                    log.warn(msg, ex) // send to user via http socket
                }
            }
        }
    }

    /**
     * Paginate through taxon docs in SOLR and update their occurrence status via either:
     * - checking against a list of datasetID codes (config)
     * OR
     * - searching for occurrence records with the (taxon concept) GUID
     *
     * Example cursor search
     * http://bie-dev.ala.org.au/solr/bie/select?q=idxtype:TAXON+AND+taxonomicStatus:accepted&wt=json&rows=100&indent=true&sort=id+asc&cursorMark=*
     * Pagination via cursor: https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results
     **/
    def importOccurrenceData() throws Exception {
        String nationalSpeciesDatasets = grailsApplication.config.nationalSpeciesDatasets // comma separated String
        def pageSize = BATCH_SIZE
        def paramsMap = [
                q: "taxonomicStatus:accepted", // "taxonomicStatus:accepted",
                //fq: "datasetID:dr2699", // testing only with AFD
                cursorMark: "*", // gets updated by subsequent searches
                fl: "id,idxtype,guid,scientificName,datasetID", // will restrict results to dos with these fields (bit like fq)
                rows: pageSize,
                sort: "id asc", // needed for cursor searching
                wt: "json"
        ]

        // first get a count of results so we can determine number of pages to process
        Map countMap = paramsMap.clone(); // shallow clone is OK
        countMap.rows = 0
        countMap.remove("cursorMark")
        def searchCount = searchService.getCursorSearchResults(new MapSolrParams(countMap), true) // could throw exception
        def totalDocs = searchCount?.response?.numFound?:0
        int totalPages = (totalDocs + pageSize - 1) / pageSize
        log.debug "totalDocs = ${totalDocs} || totalPages = ${totalPages}"
        log("Processing " + String.format("%,d", totalDocs) + " taxa (via ${paramsMap.q})...<br>") // send to browser

        def promiseList = new PromiseList() // for biocache queries
        Queue commitQueue = new ConcurrentLinkedQueue()  // queue to put docs to be indexes
        ExecutorService executor = Executors.newSingleThreadExecutor() // consumer of queue - single blocking thread
        executor.execute {
            indexDocInQueue(commitQueue, "initialised") // will keep polling the queue until terminated via cancel()
        }

        // iterate over pages
        (1..totalPages).each { page ->
            try {
                MapSolrParams solrParams = new MapSolrParams(paramsMap)
                log.debug "${page}. paramsMap = ${paramsMap}"
                def searchResults = searchService.getCursorSearchResults(solrParams, true) // use offline index to search
                def resultsDocs = searchResults?.response?.docs?:[]

                // buckets to group results into
                def taxaLocatedInHubCountry = []  // automatically get included
                def taxaToSearchOccurrences = []  // need to search biocache to see if they are located in hub country

                // iterate over the result set
                resultsDocs.each { doc ->
                    if (nationalSpeciesDatasets && nationalSpeciesDatasets.contains(doc.datasetID)) {
                        taxaLocatedInHubCountry.add(doc) // in national list so _assume_ it is located in host/hub county
                    } else {
                        taxaToSearchOccurrences.add(doc) // search occurrence records to determine if it is located in host/hub county
                    }
                }

                // update national list without occurrence record lookup
                updateTaxaWithLocationInfo(taxaLocatedInHubCountry, commitQueue)
                // update the rest via occurrence search (non blocking via promiseList)
                promiseList << { searchOccurrencesWithGuids(resultsDocs, commitQueue) }
                // update cursor
                paramsMap.cursorMark = searchResults?.nextCursorMark?:""
                // update view via via JS
                updateProgressBar(totalPages, page)
                log("${page}. taxaLocatedInHubCountry = ${taxaLocatedInHubCountry.size()} | taxaToSearchOccurrences = ${taxaToSearchOccurrences.size()}")
            } catch (Exception ex) {
                log.warn "Error calling BIE SOLR: ${ex.message}", ex
                log("ERROR calling SOLR: ${ex.message}")
            }
        }

        log("Waiting for all occurrence searches and SOLR commits to finish (could take some time)")

        //promiseList.get() // block until all promises are complete
        promiseList.onComplete { List results ->
            //executor.shutdownNow()
            isKeepIndexing = false // stop indexing thread
            executor.shutdown()
            log("Total taxa found with occurrence records = ${results.sum()}")
            log("waiting for indexing to finish...")
        }
    }

    /**
     * Batch update of SOLR docs for occurrence/location info
     * TODO extract field name into config: "locatedInHubCountry"
     *
     * @param docs
     * @param commitQueue
     * @return
     */
    def updateTaxaWithLocationInfo(List docs, Queue commitQueue) {
        def totalDocumentsUpdated = 0

        docs.each { Map doc ->
            if (doc.containsKey("id") && doc.containsKey("guid") && doc.containsKey("idxtype")) {
                Map updateDoc = [:]
                updateDoc["id"] = doc.id // doc key
                updateDoc["idxtype"] = ["set": doc.idxtype] // required field
                updateDoc["guid"] = ["set": doc.guid] // required field
                updateDoc["locatedInHubCountry"] = ["set": true]
                if(doc.containsKey("occurrenceCount")){
                    updateDoc["occurrenceCount"] = ["set": doc["occurrenceCount"]]
                }
                commitQueue.offer(updateDoc) // throw it on the queue
                totalDocumentsUpdated++
            } else {
                log.warn "Updating doc error: missing keys ${doc}"
            }
        }

        totalDocumentsUpdated
    }

    /**
     * Poll the queue of docs and index in batches
     *
     * @param updateDocs
     * @return
     */
    def indexDocInQueue(Queue updateDocs, msg) {
        int batchSize = BUFFER_SIZE

        while (isKeepIndexing || updateDocs.size() > 0) {
            if (updateDocs.size() > 0) {
                log.info "Starting indexing of ${updateDocs.size()} docs"
                try {
                    // batch index docs
                    List batchDocs = []
                    int end = (batchSize < updateDocs.size()) ? batchSize : updateDocs.size()

                    (1..end).each {
                        if (updateDocs.peek()) {
                            batchDocs.add(updateDocs.poll())
                        }
                    }

                    indexService.indexBatch(batchDocs) // index
                } catch (Exception ex) {
                    log.warn "Error batch indexing: ${ex.message}", ex
                    log.warn "updateDocs = ${updateDocs}"
                    log("ERROR batch indexing: ${ex.message} <br><code>${ex.stackTrace}</code>")
                }
            } else {
                sleep(500)
            }
        }

        log("Indexing thread is done: ${msg}")
    }

    /**
     * Extract a list of GUIDs from input list of docs and do paginated/batch search of occurrence records,
     * updating index with occurrence status info (could be presence or record counts, etc)
     *
     * @param docs
     * @return
     */
    def searchOccurrencesWithGuids(List docs, Queue commitQueue) {
        int batchSize = 20 // even with POST SOLR throws 400 code is batchSize is more than 100
        List guids = docs.collect { it.guid }
        int totalPages = ((guids.size() + batchSize - 1) / batchSize) -1
        log.debug "total = ${guids.size()} || batchSize = ${batchSize} || totalPages = ${totalPages}"
        List docsWithRecs = [] // docs to index
        //log("Getting occurrence data for ${docs.size()} docs")

        (0..totalPages).each { index ->
            int start = index * batchSize
            int end = (start + batchSize < guids.size()) ? start + batchSize - 1 : guids.size()
            log "paging biocache search - ${start} to ${end}"
            def guidSubset = guids.subList(start,end)
            def guidParamList = guidSubset.collect { String guid -> ClientUtils.escapeQueryChars(guid) } // URL encode guids
            def query = "taxon_concept_lsid:" + guidParamList.join("+OR+taxon_concept_lsid:")
            def filterQuery = grailsApplication.config.occurrenceCounts.filterQuery

            try {
                // def json = searchService.doPostWithParamsExc(grailsApplication.config.biocache.solr.url +  "/select", postBody)
                // log.debug "results = ${json?.resp?.response?.numFound}"
                def url = grailsApplication.config.biocache.solr.url + "/select?q=${query}&fq=${filterQuery}&" +
                        "wt=json&indent=true&rows=0&facet=true&facet.field=taxon_concept_lsid&facet.mincount=1"
                def queryResponse = new URL(Encoder.encodeUrl(url)).getText("UTF-8")
                JSONObject jsonObj = JSON.parse(queryResponse)

                if (jsonObj.containsKey("facet_counts")) {

                    def facetCounts = jsonObj?.facet_counts?.facet_fields?.taxon_concept_lsid
                    facetCounts.eachWithIndex { val, idx ->
                        // facets results are a list with key, value, key, value, etc
                        if (idx % 2 == 0) {
                            def docWithRecs = docs.find { it.guid == val }
                            docWithRecs["occurrenceCount"] = facetCounts[idx + 1] //add the count
                            if(docWithRecs){
                                docsWithRecs.add(docWithRecs )
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                log.warn "Error calling biocache SOLR: ${ex.message}", ex
                log("ERROR calling biocache SOLR: ${ex.message}")
            }
        }

        if (docsWithRecs.size() > 0) {
            log.debug "docsWithRecs size = ${docsWithRecs.size()} vs docs size = ${docs.size()}"
            updateTaxaWithLocationInfo(docsWithRecs, commitQueue)
        }

    }

    /**
     * Update TAXON SOLR doc with conservation status info
     *
     * @param json
     * @param jsonFieldName
     * @param SolrFieldName
     * @return
     */
    private updateDocsWithConservationStatus(JSONElement json, String jsonFieldName, String SolrFieldName, String drUid, String action, String speciesListName) {
        if (json.size() > 0) {
            def totalDocs = json.size()
            def buffer = []
            def statusMap = vernacularNameStatus()
            def legistatedStatusType = statusMap.get("legislated")
            def unmatchedTaxaCount = 0

            updateProgressBar2(100, 0)
            log("Updating taxa with ${SolrFieldName}")
            json.eachWithIndex { item, i ->
                log.debug "item = ${item}"
                def taxonDoc

                if (item.lsid) {
                    taxonDoc = searchService.lookupTaxon(item.lsid, true) // TODO cache call
                }

                if (!taxonDoc && item.name) {
                    taxonDoc = searchService.lookupTaxonByName(item.name, true) // TODO cache call
                }

                if (taxonDoc) {
                    // do a SOLR doc (atomic) update
                    def doc = [:]
                    doc["id"] = taxonDoc.id // doc key
                    doc["idxtype"] = ["set": taxonDoc.idxtype] // required field
                    doc["guid"] = ["set": taxonDoc.guid] // required field
                    def fieldVale
                    if (jsonFieldName == "*") { //note membership of list itself, rather than specific list field value
                        if (speciesListName) {
                            fieldVale = speciesListName
                        } else {
                            fieldVale = drUid
                        }
                    } else {
                        fieldVale = item.kvpValues.find { it.key == jsonFieldName }?.get("value")
                    }
                    if (action == "set") {
                        doc[SolrFieldName] = ["set": fieldVale]
                    } else if (action == "add") {
                        ArrayList<String> existingVals = taxonDoc[SolrFieldName]
                        if (!existingVals) {
                            doc[SolrFieldName] = ["set": fieldVale]
                        } else {
                            if (!existingVals.contains(fieldVale)) existingVals << fieldVale
                            doc[SolrFieldName] = ["set": existingVals]
                        }
                    }
                    log.debug "adding to doc = ${doc}"
                    buffer << doc
                } else {
                    // No match so add it as a vernacular name
                    def capitaliser = TitleCapitaliser.create(grailsApplication.config.commonNameDefaultLanguage)
                    def doc = [:]
                    doc["id"] = UUID.randomUUID().toString() // doc key
                    doc["idxtype"] = IndexDocType.TAXON // required field
                    doc["guid"] = "ALA_${item.name?.replaceAll("[^A-Za-z0-9]+", "_")}" // replace non alpha-numeric chars with '_' - required field
                    doc["datasetID"] = drUid
                    doc["datasetName"] = "Conservation list for ${SolrFieldName}"
                    doc["name"] = capitaliser.capitalise(item.name)
                    doc["status"] = legistatedStatusType?.status ?: "legistated"
                    doc["priority"] = legistatedStatusType?.priority ?: 500
                    // set conservationStatus facet
                    def fieldVale = item.kvpValues.find { it.key == jsonFieldName }?.get("value")
                    doc[SolrFieldName] = fieldVale
                    log.debug "new name doc = ${doc}"
                    buffer << doc
                    log("No existing taxon found for ${item.name}, so has been added as ${doc["guid"]}")
                }

                if (i > 0) {
                    updateProgressBar2(totalDocs, i)
                }
            }

            log("Committing to SOLR...")
            indexService.indexBatch(buffer)
            updateProgressBar2(100, 100)
            log("Number of taxa unmatched: ${unmatchedTaxaCount}")
            log("Import finished.")
        } else {
            log("JSON not an array or has no elements - exiting")
        }
    }

    private void importAdditionalVernacularNames(JSONElement json, String vernacularNameField, String nameIdField, String statusField, String languageField, String sourceField, String resourceLanguage, String uid) {
        if (json.size() > 0) {
            def totalDocs = json.size()
            def buffer = []
            def statusMap = vernacularNameStatus()
            def commonStatus = statusMap.get("common")
            def unmatchedTaxaCount = 0

            updateProgressBar2(100, 0)
            log("Updating vernacular names from ${uid}")
            json.eachWithIndex { item, i ->
                log.debug "item = ${item}"
                def vernacularName = item.kvpValues.find { it.key == vernacularNameField }?.get("value")
                def nameId = item.kvpValues.find { it.key == nameIdField }?.get("value")
                def status = statusMap[item.kvpValues.find { it.key == statusField }?.get("value")]
                def language = item.kvpValues.find { it.key == languageField }?.get("value") ?: resourceLanguage
                def source = item.kvpValues.find { it.key == sourceField }?.get("value")

                if (!addVernacularName(item.lsid, item.name, vernacularName, nameId, status, language, source, uid, null, null, [:], buffer, commonStatus))
                    unmatchedTaxaCount++

                if (i > 0) {
                    updateProgressBar2(totalDocs, i)
                }
            }
            log("Committing names to SOLR...")
            if (!buffer.isEmpty())
                indexService.indexBatch(buffer)
            log("Ensure denormalisation is re-run")
        } else {
            log("JSON not an array or has no elements - exiting")
        }

    }


    private boolean addVernacularName(String taxonID, String name, String vernacularName, String nameId, Object status, String language, String source, String datasetID, String taxonRemarks, String provenance, Map additional, List buffer, Object defaultStatus) {
        def taxonDoc = null

        if (taxonID)
            taxonDoc = searchService.lookupTaxon(taxonID, true)
        if (!taxonDoc && name)
            taxonDoc = searchService.lookupTaxonByName(name, true)
        if (!taxonDoc) {
            log.warn("Can't find matching taxon document for ${taxonID} for ${vernacularName}, skipping")
            return false
        }
        def capitaliser = TitleCapitaliser.create(language ?: grailsApplication.config.commonNameDefaultLanguage)
        vernacularName = capitaliser.capitalise(vernacularName)
        def remarksList = taxonRemarks?.split("\\|").collect({ it.trim() })
        def provenanceList = provenance?.split("\\|").collect({ it.trim()})
        def vernacularDoc = searchService.lookupVernacular(taxonDoc.guid, vernacularName, true)
        if (vernacularDoc) {
            // do a SOLR doc (atomic) update
            def doc = [:]
            doc["id"] = vernacularDoc.id // doc key
            doc["idxtype"] = ["set": vernacularDoc.idxtype] // required field
            doc["guid"] = ["set": vernacularDoc.guid] // required field
            doc["taxonGuid"] = ["set": taxonDoc.guid]
            doc["name"] = ["set": vernacularName]
            doc["datasetID"] = ["set": datasetID]
            doc["language"] = ["set": language]
            if (nameId)
                doc["nameID"] = ["set": nameId]
            if (status) {
                doc["status"] = ["set": status.status]
                doc["priority"] = ["set": status.priority]
            }
            if (source)
                doc["source"] = ["set": source]
            if (remarksList)
                doc["taxonRemarks"] = ["set": remarksList]
            if (provenanceList)
                doc["provenance"] = ["set": provenanceList]
            additional.each { k, v -> doc[k] = ["set": v] }
            log.debug "adding to doc = ${doc}"
            buffer << doc
        } else {
            // No match so add it as a vernacular name
            def doc = [:]
            doc["id"] = UUID.randomUUID().toString() // doc key
            doc["idxtype"] = IndexDocType.COMMON // required field
            doc["guid"] = doc.id
            doc["taxonGuid"] = taxonDoc.guid
            doc["datasetID"] = datasetID
            doc["name"] = vernacularName
            doc["status"] = status?.status ?: defaultStatus.status
            doc["priority"] = status?.priority ?: defaultStatus.priority
            doc["nameID"] = nameId
            doc["language"] = language
            if (source)
                doc["source"] = source
            if (remarksList)
                doc["taxonRemarks"] = remarksList
            if (provenanceList)
                doc["provenance"] = provenanceList
            additional.each { k, v -> doc[k] = v }
            log.debug "new name doc = ${doc} for ${vernacularName}"
            buffer << doc
        }
        return true
    }

    def clearDanglingSynonyms(){
        log("Starting clear dangling synonyms")
        indexService.deleteFromIndexByQuery("taxonomicStatus:synonym AND -acceptedConceptName:*")
        log("Finished clear dangling synonyms")
    }

    def clearTaxaIndex() {
        log("Deleting existing taxon entries in index...")
        indexService.deleteFromIndex(IndexDocType.TAXON)
        indexService.deleteFromIndex(IndexDocType.COMMON)
        indexService.deleteFromIndex(IndexDocType.IDENTIFIER)
        indexService.deleteFromIndex(IndexDocType.TAXONVARIANT)
        log("Cleared.")
    }

    def importDwcA(dwcDir, clearIndex) {
        try {
            log("Importing archive from path.." + dwcDir)
            //read the DwC metadata
            Archive archive = ArchiveFactory.openArchive(new File(dwcDir));
            // Archive metadata available
            log("Archive metadata detected: " + (archive.metadataLocation != null))
            def defaultDatasetName = null
            if (archive.metadataLocation) {
                defaultDatasetName = archive.metadata.title?.trim()
                log("Default dataset name from metadata: " + defaultDatasetName)
            }
            //retrieve datasets
            def datasetMap = [:]
            def attributionMap = [:]
            //dataset extension available?
            ArchiveFile datasetArchiveFile = archive.getExtension(DcTerm.rightsHolder)
            log("Dataset extension detected: " + (datasetArchiveFile != null))
            if (datasetArchiveFile) {
                attributionMap = readAttribution(datasetArchiveFile)
                log("Datasets read: " + attributionMap.size())
            }
            def rowType = archive.core.rowType
            //clear
            if (clearIndex) {
                clearTaxaIndex()
            } else {
                log("Skipping deleting existing entries in index...")
            }
            if (rowType == DwcTerm.Taxon)
                importTaxonDwcA(archive, attributionMap, datasetMap, defaultDatasetName)
            else if (rowType == GbifTerm.VernacularName)
                importVernacularDwcA(archive.core, attributionMap, datasetMap, defaultDatasetName)
            else if (rowType == GbifTerm.Identifier)
                importIdentifierDwcA(archive.core, attributionMap, datasetMap, defaultDatasetName)
            else if (rowType == ALATerm.TaxonVariant)
                importTaxonVariantDwcA(archive.core, attributionMap, datasetMap, defaultDatasetName)
            else
                log("Unable to import an archive of type " + rowType)
            def variantExtension = archive.getExtension(ALATerm.TaxonVariant)
            if (variantExtension)
                importTaxonVariantDwcA(variantExtension, attributionMap, datasetMap, defaultDatasetName)
            def vernacularExtension = archive.getExtension(GbifTerm.VernacularName)
            if (vernacularExtension)
                importVernacularDwcA(vernacularExtension, attributionMap, datasetMap, defaultDatasetName)
            def identifierExtension = archive.getExtension(GbifTerm.Identifier)
            if (identifierExtension)
                importIdentifierDwcA(identifierExtension, attributionMap, datasetMap, defaultDatasetName)
            log("Import finished.")
        } catch (Exception ex) {
            log("There was problem with the import: " + ex.getMessage())
            log("See server logs for more details.")
            log.error(ex.getMessage(), ex)
        }

    }

    /**
     * Import a taxon DwC-A into this system.
     *
     * @return
     */
    def importTaxonDwcA(Archive archive, Map attributionMap, Map datasetMap, String defaultDatasetName) {
        /*
        log.info("Loading Species Group mappings for DwcA import")
        def speciesGroupMapping = speciesGroupService.invertedSpeciesGroups
        log.info("Finished loading Species Group mappings")
        */
        log("Importing taxa")

        //read the DwC metadata
        ArchiveFile taxaArchiveFile = archive.getCore()

        //retrieve taxon rank mappings
        log("Reading taxon ranks..")
        def taxonRanks = ranks()
        log("Reading taxon ranks.." + taxonRanks.size() + " read.")

        //compile a list of synonyms into memory....
        //def synonymMap = readSynonyms(taxaArchiveFile, taxonRanks)
        //log("Synonyms read for " + synonymMap.size() + " taxa")

        log("Creating entries in index...")


        def buffer = []
        def counter = 0

        Iterator<StarRecord> iter = archive.iterator()

        while (iter.hasNext()) {

            StarRecord record = iter.next()
            Record core = record.core()

            def taxonID = core.id()
            def acceptedNameUsageID = core.value(DwcTerm.acceptedNameUsageID)
            def synonym = taxonID != acceptedNameUsageID && acceptedNameUsageID != "" && acceptedNameUsageID != null
            def parentNameUsageID = core.value(DwcTerm.parentNameUsageID)
            def defaultTaxonomicStatus = synonym ? "inferredSynonym" : "inferredAaccepted"

            def doc = ["idxtype": IndexDocType.TAXON.name()]
            doc["id"] = UUID.randomUUID().toString()
            doc["guid"] = taxonID
            doc["parentGuid"] = parentNameUsageID
            if (synonym) {
                doc["acceptedConceptID"] = acceptedNameUsageID
            } else {
                // Filled out during denormalisation
                doc['speciesGroup'] = []
                doc['speciesSubgroup'] = []
            }
            buildTaxonRecord(core, doc, attributionMap, datasetMap, taxonRanks, defaultTaxonomicStatus, defaultDatasetName)

            if (record.hasExtension(GbifTerm.Distribution)) {
                record.extension(GbifTerm.Distribution).each {
                    def distribution = it.value(DwcTerm.stateProvince)
                    if (distribution)
                        doc["distribution"] = distribution
                }
            }

            buffer << doc
            counter++
            if (buffer.size() >= BUFFER_SIZE) {
                if (counter % REPORT_INTERVAL == 0)
                    log("Adding taxa: ${counter}")
                indexService.indexBatch(buffer)
                buffer.clear()
            }
        }

        if (!buffer.isEmpty()) {
            log("Adding taxa: ${counter}")
            indexService.indexBatch(buffer)
        }
    }


    def importVernacularDwcA(ArchiveFile archiveFile, Map attributionMap, Map datasetMap, String defaultDatasetName) throws Exception {
        if (archiveFile.rowType != GbifTerm.VernacularName)
            throw new IllegalArgumentException("Vernacular import only works for files of type " + GbifTerm.VernacularName + " got " + archiveFile.rowType)
        log("Importing vernacular names")
        def statusMap = vernacularNameStatus()
        def defaultStatus = statusMap.get("common")
        def preferredStatus = statusMap.get('preferred')
        String defaultLanguage = grailsApplication.config.commonNameDefaultLanguage
        def buffer = []
        def count = 0
        for (Record record: archiveFile) {
            String taxonID = record.id()
            String vernacularName = record.value(DwcTerm.vernacularName)
            String nameID = record.value(ALATerm.nameID)
            Object status = statusMap.get(record.value(ALATerm.status))
            String language = record.value(DcTerm.language) ?: defaultLanguage
            String source = record.value(DcTerm.source)
            String datasetID = record.value(DwcTerm.datasetID)
            String temporal = record.value(DcTerm.temporal)
            String locationID = record.value(DwcTerm.locationID)
            String locality = record.value(DwcTerm.locality)
            String countryCode = record.value(DwcTerm.countryCode)
            String sex = record.value(DwcTerm.sex)
            String lifeStage = record.value(DwcTerm.lifeStage)
            String isPlural = record.value(GbifTerm.isPlural)
            String isPreferred = record.value(GbifTerm.isPreferredName)
            if (!status && isPreferred && isPreferred.toBoolean())
                status = preferredStatus
            String organismPart = record.value(GbifTerm.organismPart)
            String taxonRemarks = record.value(DwcTerm.taxonRemarks)
            def remarksList = taxonRemarks?.split("\\|").collect({ it.trim() })
            String provenance = record.value(DcTerm.provenance)
            def provenanceList = provenance?.split("\\|").collect({ it.trim() })
            String labels = record.value(ALATerm.labels)
            def capitaliser = TitleCapitaliser.create(language ?: defaultLanguage)
            vernacularName = capitaliser.capitalise(vernacularName)
            def doc = [:]
            doc["id"] = UUID.randomUUID().toString() // doc key
            doc["idxtype"] = IndexDocType.COMMON // required field
            doc["guid"] = doc.id
            doc["taxonGuid"] = taxonID
            doc["datasetID"] = datasetID
            doc["name"] = vernacularName
            doc["status"] = status?.status ?: defaultStatus.status
            doc["priority"] = status?.priority ?: defaultStatus.priority
            doc["nameID"] = nameID
            doc["language"] = language ?: defaultLanguage
            doc["source"] = source
            doc["temporal"] = temporal
            doc["locationID"] = locationID
            doc["locality"] = locality
            doc["countryCode"] = countryCode
            doc["sex"] = sex
            doc["lifeStage"] = lifeStage
            doc["isPlural"] = isPlural
            doc["organismPart"] = organismPart
            doc["taxonRemarks"] = remarksList
            doc["provenance"] = provenanceList
            doc["labels"] = labels
            doc["distribution"] = "N/A"
            def attribution = findAttribution(datasetID, attributionMap, datasetMap)
            if (attribution) {
                doc["datasetName"] = attribution["datasetName"]
                doc["rightsHolder"] = attribution["rightsHolder"]
            } else if (defaultDatasetName) {
                doc["datasetName"] = defaultDatasetName
            }
            buffer << doc
            count++
            if (buffer.size() >= BUFFER_SIZE) {
                indexService.indexBatch(buffer)
                buffer.clear()
                if (count % REPORT_INTERVAL == 0)
                    log("Processed ${count} records")
            }
        }
        if (buffer.size() > 0) {
            indexService.indexBatch(buffer)
            log("Processed ${count} records")
        }
    }


    def importIdentifierDwcA(ArchiveFile archiveFile, Map attributionMap, Map datasetMap, String defaultDatasetName) throws Exception {
        if (archiveFile.rowType != GbifTerm.Identifier)
            throw new IllegalArgumentException("Identifier import only works for files of type " + GbifTerm.Identifier + " got " + archiveFile.rowType)
        log("Importing identifiers")
        def statusMap = identifierStatus()
        def defaultStatus = statusMap.get("unknown")
        def buffer = []
        def count = 0
        for (Record record: archiveFile) {
            def taxonID = record.id()
            def identifier = record.value(DcTerm.identifier)
            def title = record.value(DcTerm.title)
            def subject = record.value(DcTerm.subject)
            def format = record.value(DcTerm.format)
            def source = record.value(DcTerm.source)
            def datasetID = record.value(DwcTerm.datasetID)
            def idStatus = record.value(ALATerm.status)
            def status = idStatus ? statusMap.get(idStatus.toLowerCase()) : null
            String provenance = record.value(DcTerm.provenance)
            def provenanceList = provenance?.split("\\|").collect({ it.trim() })

            def doc = [:]
            doc["id"] = UUID.randomUUID().toString() // doc key
            doc["idxtype"] = IndexDocType.IDENTIFIER // required field
            doc["guid"] = identifier
            doc["taxonGuid"] = taxonID
            doc["datasetID"] = datasetID
            doc["status"] = status?.status ?: defaultStatus.status
            doc["priority"] = status?.priority ?: defaultStatus.priority
            doc["name"] = title
            doc["subject"] = subject
            doc["format"] = format
            doc["source"] = source
            doc["provenance"] = provenanceList
            def attribution = findAttribution(datasetID, attributionMap, datasetMap)
            if (attribution) {
                doc["datasetName"] = attribution["datasetName"]
                doc["rightsHolder"] = attribution["rightsHolder"]
            } else if (defaultDatasetName) {
                doc["datasetName"] = defaultDatasetName
            }
            buffer << doc
            count++
            if (buffer.size() >= BUFFER_SIZE) {
                indexService.indexBatch(buffer)
                buffer.clear()
                if (count % REPORT_INTERVAL == 0)
                    log("Processed ${count} records")
            }
        }
        if (buffer.size() > 0) {
            indexService.indexBatch(buffer)
            log("Processed ${count} records")
        }
    }

    def importTaxonVariantDwcA(ArchiveFile archiveFile, Map attributionMap, Map datasetMap, String defaultDatasetName) throws Exception {
        if (archiveFile.rowType != ALATerm.TaxonVariant)
            throw new IllegalArgumentException("Taxon variant import only works for files of type " + ALATerm.TaxonVariant + " got " + archiveFile.rowType)
        log("Importing taxon variants")
        def taxonRanks = ranks()
        def buffer = []
        def count = 0
        for (Record record: archiveFile) {
            def doc = [:]
            doc["id"] = UUID.randomUUID().toString() // doc key
            doc["idxtype"] = IndexDocType.TAXONVARIANT.name() // required field
            doc["taxonGuid"] = record.id()
            doc["guid"] = record.value(DwcTerm.taxonID)
            buildTaxonRecord(record, doc, attributionMap, datasetMap, taxonRanks, "inferredAccepted", defaultDatasetName)
            buffer << doc
            count++
            if (buffer.size() >= BUFFER_SIZE) {
                indexService.indexBatch(buffer)
                buffer.clear()
                if (count % REPORT_INTERVAL == 0)
                    log("Processed ${count} records")
            }
        }
        if (buffer.size() > 0) {
            indexService.indexBatch(buffer)
            log("Processed ${count} records")
        }
    }

    def buildTaxonRecord(Record record, Map doc, Map attributionMap, Map datasetMap, Map taxonRanks, String defaultTaxonomicStatus, String defaultDatasetName) {
        def datasetID = record.value(DwcTerm.datasetID)
        def taxonRank = (record.value(DwcTerm.taxonRank) ?: "").toLowerCase()
        def scientificName = record.value(DwcTerm.scientificName)
        def parentNameUsageID = record.value(DwcTerm.parentNameUsageID)
        def scientificNameAuthorship = record.value(DwcTerm.scientificNameAuthorship)
        def nameComplete = record.value(ALATerm.nameComplete)
        def nameFormatted = record.value(ALATerm.nameFormatted)
        def taxonRankID = taxonRanks.get(taxonRank) ? taxonRanks.get(taxonRank).rankID : -1
        def taxonomicStatus = record.value(DwcTerm.taxonomicStatus) ?: defaultTaxonomicStatus
        String taxonRemarks = record.value(DwcTerm.taxonRemarks)
        String provenance = record.value(DcTerm.provenance)
        def remarksList = taxonRemarks?.split("\\|").collect({ it.trim() })
        def provenanceList = provenance?.split("\\|").collect({ it.trim() })

        doc["datasetID"] = datasetID
        doc["parentGuid"] = parentNameUsageID
        doc["rank"] = taxonRank
        //only add the ID if we have a recognised rank
        if(taxonRankID > 0){
            doc["rankID"] = taxonRankID
        }
        doc["scientificName"] = scientificName
        doc["scientificNameAuthorship"] = scientificNameAuthorship
        doc["nameComplete"] = buildNameComplete(nameComplete, scientificName, scientificNameAuthorship)
        doc["nameFormatted"] = buildNameFormatted(nameFormatted, nameComplete, scientificName, scientificNameAuthorship, taxonRank, taxonRanks)
        doc["taxonomicStatus"] = taxonomicStatus
        doc["taxonRemarks"] = taxonRemarks
        doc["provenance"] = provenanceList

        //index additional fields that are supplied in the record
        record.terms().each { term ->
            if (!TAXON_ALREADY_INDEXED.contains(term)) {
                if (IN_SCHEMA.contains(term)) {
                    doc[term.simpleName()] = record.value(term)
                } else {
                    //use a dynamic field extension
                    doc[term.simpleName() + DYNAMIC_FIELD_EXTENSION] = record.value(term)
                }
            }
        }

        def attribution = findAttribution(datasetID, attributionMap, datasetMap)
        if (attribution) {
            doc["datasetName"] = attribution["datasetName"]
            doc["rightsHolder"] = attribution["rightsHolder"]
        } else if (defaultDatasetName) {
            doc["datasetName"] = defaultDatasetName
        }
    }


    static String normaliseRank(String rank) {
        return rank?.toLowerCase()?.replaceAll("[^a-z]", "_")
    }

    /**
     * Read the attribution file, building a map of ID -> name, dataProvider
     *
     * @param fileName
     * @return
     */
    private def readAttribution(ArchiveFile datasetArchiveFile) {

        def datasets = [:]
        if (!datasetArchiveFile) {
            return datasets
        }

        Iterator<Record> iter = datasetArchiveFile.iterator()
        while (iter.hasNext()) {
            Record record = iter.next()
            def datasetID = record.id()
            def datasetName = record.value(DwcTerm.datasetName)
            def rightsHolder = record.value(DcTerm.rightsHolder)
            datasets.put(datasetID, [datasetName: datasetName, rightsHolder: rightsHolder])
        }
        datasets
    }

    /**
     * Go through the index and build link identifiers for unique names.
     */
    def buildLinkIdentifiers(online) {
        int pageSize = BUFFER_SIZE
        int page = 0
        int added = 0
        def js = new JsonSlurper()
        def baseUrl = online ? grailsApplication.config.indexLiveBaseUrl : grailsApplication.config.indexOfflineBaseUrl
        def typeQuery = "idxtype:\"" + IndexDocType.TAXON.name() + "\"+OR+idxtype:\"" + IndexDocType.COMMON.name() + "\""

        js.setType(JsonParserType.INDEX_OVERLAY)
        log("Starting link identifier scan")
        try {
            while (true) {
                def startTime = System.currentTimeMillis()
                def solrServerUrl = baseUrl + "/select?wt=json&q=" + typeQuery + "&start=" + (pageSize * page) + "&rows=" + pageSize
                def queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                def json = js.parseText(queryResponse)
                int total = json.response.numFound
                def docs = json.response.docs
                def buffer = []

                if (docs.isEmpty())
                    break
                docs.each { doc ->
                    def name = doc.scientificName ?: doc.name
                    try {
                        if (name) {
                            String encName = ClientUtils.escapeQueryChars(name)
                            String nameSearchUrl = baseUrl + "/select?wt=json&q=name:" + encName + "+OR+scientificName:" + encName + "&fq=" + typeQuery + "&rows=0"
                            def nameResponse = Encoder.encodeUrl(nameSearchUrl).toURL().getText("UTF-8")
                            def nameJson = js.parseText(nameResponse)
                            int found = nameJson.response.numFound
                            if (found == 1) {
                                //log.debug("Adding link identifier for ${name} to ${doc.id}")
                                def update = [:]
                                update["id"] = doc.id // doc key
                                update["idxtype"] = ["set": doc.idxtype] // required field
                                update["guid"] = ["set": doc.guid] // required field
                                update["linkIdentifier"] = ["set": name]
                                buffer << update
                                added++
                            }
                        }
                    } catch (Exception ex) {
                        log.warn "Unable to search for name ${name}: ${ex.message}"
                    }
                }
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                page++
                if (page % 10 == 0) {
                    def progress = page * pageSize
                    def percentage = Math.round(progress * 100 / total)
                    def speed = Math.round((page * 1000) / (System.currentTimeMillis() - startTime))
                    log("Processed ${page * pageSize} names (${percentage}%), added ${added} unique links, ${speed} names per second")
                }
            }
            log("Finished scan")
        } catch (Exception ex) {
            log.error("Unable to perform link identifier scan", ex)
            log("Error during scan: " + ex.getMessage())
        }
    }

    /**
     * Go through the index and build image links for taxa
     */
    def loadImages(online) {
        int pageSize = BATCH_SIZE
        int processed = 0
        int added = 0
        def js = new JsonSlurper()
        def baseUrl = online ? grailsApplication.config.indexLiveBaseUrl : grailsApplication.config.indexOfflineBaseUrl
        def biocacheSolrUrl = grailsApplication.config.biocache.solr.url
        def acceptedQuery = TaxonomicType.values().findAll({ it.accepted }).collect({"taxonomicStatus:${it.term}"}).join('+OR+')
        def typeQuery = "idxtype:\"${IndexDocType.TAXON.name()}\"+AND+($acceptedQuery)"
        def prevCursor = ""
        def cursor = "*"
        def listConfig = this.getConfigFile(grailsApplication.config.imagesListsUrl)
        def imageMap = collectImageLists(listConfig.lists)
        def rankMap = listConfig.ranks.collectEntries { r -> [(r.rank): r] }
        def boosts = listConfig.boosts.collect({"bq=" + it}).join("&")
        def imageFields = listConfig.imageFields?:IMAGE_FIELDS
        log.debug "listConfig = ${listConfig} || imageFields = ${listConfig.imageFields}"
        def lastImage = [imageId: "none", taxonID: "none", name: "none"]
        def addImageSearch = { query, field, value, boost ->
            if (field && value) {
                value = ClientUtils.escapeQueryChars(value)
                query = query ? query + " OR " : ""
                query = query + "${field}:${value}^${boost}"
            }
            query
        }

        js.setType(JsonParserType.INDEX_OVERLAY)
        log("Starting image load scan for ${online ? 'online' : 'offline'} index")
        try {
            while (prevCursor != cursor) {
                def startTime = System.currentTimeMillis()
                def solrServerUrl = baseUrl + "/select?wt=json&q=" + typeQuery + "&cursorMark=" + cursor + "&sort=id+asc&rows=" + pageSize
                def queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                def json = js.parseText(queryResponse)
                int total = json.response.numFound
                def docs = json.response.docs
                def buffer = []

                docs.each { doc ->
                    def taxonID = doc.guid
                    def kingdom = doc.rk_kingdom
                    def name = doc.scientificName ?: doc.name
                    def rank = rankMap[doc.rank]
                    def image = null

                    if (rank != null) {
                        try {
                            image = imageMap[taxonID] ?: imageMap[name]
                            if (!image) {
                                def query = null
                                query = addImageSearch(query, "lsid", taxonID, 100)
                                query = addImageSearch(query, rank.idField, taxonID, 20)
                                if (query) {
                                    def taxonSearchUrl = biocacheSolrUrl + "/select?q=(${query}) AND multimedia:Image&${boosts}&rows=5&wt=json&fl=${imageFields}"
                                    //def taxonResponse = Encoder.encodeUrl(taxonSearchUrl).toURL().getText("UTF-8")
                                    def taxonResponse = Encoder.encodeUrl(taxonSearchUrl).toURL().getText("UTF-8")
                                    def taxonJson = js.parseText(taxonResponse)
                                    if (taxonJson.response.numFound > 0) {
                                        // Case does not necessarily match between bie and biocache
                                        def occurrence = taxonJson.response.docs.find { !kingdom || kingdom.equalsIgnoreCase(it.kingdom) }
                                        if (occurrence)
                                            image = [taxonID: taxonID, name: name, imageId: occurrence.image_url]
                                    }
                                }
                            }
                        } catch (Exception ex) {
                            log.warn "Unable to search for name ${name}: ${ex.message}"
                        }
                    }
                    if (image) {
                        def update = [:]
                        update["id"] = doc.id // doc key
                        update["idxtype"] = ["set": doc.idxtype] // required field
                        update["guid"] = ["set": doc.guid] // required field
                        update["image"] = ["set": image.imageId]
                        update["imageAvailable"] = ["set": true]
                        added++
                        buffer << update
                        lastImage = image
                    }
                    processed++
                }
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                def percentage = Math.round(processed * 100 / total)
                def speed = Math.round((pageSize * 1000) / (System.currentTimeMillis() - startTime))
                log("Processed ${processed} names (${percentage}%), added ${added} images, ${speed} taxa per second. Last image ${lastImage.imageId} for ${lastImage.name}, ${lastImage.taxonID}")
                prevCursor = cursor
                cursor = json.nextCursorMark
            }
            log("Finished scan")
        } catch (Exception ex) {
            log.error("Unable to perform image scan", ex)
            log("Error during scan: " + ex.getMessage())
        }
    }

    /**
     * Collect the list where images are specifically listed
     */
    def collectImageLists(List lists) {
        def speciesListUrl = grailsApplication.config.speciesList.url
        def speciesListParams = grailsApplication.config.speciesList.params
        def imageMap = [:]
        log("Loading image lists")
        lists.each { list ->
            String drUid = list.uid
            String imageIdName = list.imageId
            String imageUrlName = list.imageUrl
            if (drUid && (imageIdName || imageUrlName)) {
                def url = "${speciesListUrl}${drUid}${speciesListParams}"
                try {
                    JSONElement json = JSON.parse(getStringForUrl(url))
                    json.each { item ->
                        def taxonID = item.lsid
                        def name = item.name
                        def imageId = imageIdName ? item.kvpValues.find { it.key == imageIdName }?.get("value") : null
                        def imageUrl = imageUrlName ? item.kvpValues.find {
                            it.key == imageUrlName
                        }?.get("value") : null
                        if (imageId || imageUrl) {
                            def image = [taxonID: taxonID, name: name, imageId: imageId, imageUrl: imageUrl]
                            if (taxonID && !imageMap.containsKey(taxonID))
                                imageMap[taxonID] = image
                            if (name && !imageMap.containsKey(name))
                                imageMap[name] = image
                        }
                    }
                } catch (Exception ex) {
                    log("Unable to load image list at ${url}: ${ex.getMessage()} ... ignoring")
                }
            }
        }
        log("Loaded image lists (${imageMap.size()} taxa)")
        return imageMap
    }

    /**
     * Updates preferred image ID via input list of (map of) GUIDs and image IDs
     *
     * @param preferredImagesList
     * @return
     */
    def updateDocsWithPreferredImage(List<Map> preferredImagesList){

        List<String> guidList = []

        preferredImagesList.each {Map guidImageMap ->
            guidList.push(guidImageMap.guid)
        }

        String guids = guidList.join(",")

        log.info ("guid List to update: " + guids)

        def paramsMap = [
                q: "guid:\"" + guids +"\"",
                wt: "json"
        ]
        def buffer = []
        MapSolrParams solrParams = new MapSolrParams(paramsMap)
        def searchResults = searchService.getCursorSearchResults(solrParams, false)
        def resultsDocs = searchResults?.response?.docs?:[]
        def totalDocumentsUpdated = 0
        resultsDocs.each { Map doc ->
            if (doc.containsKey("id") && doc.containsKey("guid") && doc.containsKey("idxtype")) {
                String imageId = getImageFromParamList(preferredImagesList, doc.guid)
                log.info ("Updating: guid " + doc.guid + " with imageId " + imageId)
                if (!doc.containsKey("image") || (doc.containsKey("image") && doc.image != imageId)) {
                    Map updateDoc = [:]
                    updateDoc["id"] = doc.id // doc key
                    updateDoc["idxtype"] = ["set": doc.idxtype] // required field
                    updateDoc["guid"] = ["set": doc.guid] // required field
                    updateDoc["image"] = ["set": imageId]
                    updateDoc["imageAvailable"] = ["set": true]
                    totalDocumentsUpdated ++
                    buffer << updateDoc
                }
            } else {
                log.warn "Updating doc error: missing keys ${doc}"
            }
        }

        def updatedTaxa = []

        if (buffer.size() > 0) {
            log.info ("Committing to SOLR..." + guidList)
            indexService.indexBatch(buffer, true)
            updatedTaxa = searchService.getTaxa(guidList)
        } else {
            log.info "Nothing to update for guidList: " + guidList
        }

        updatedTaxa
    }

    private String getImageFromParamList (List<Map> preferredImagesList, String guid) {
        return preferredImagesList.grep{it.guid == guid}.image[0]
    }

    /**
     * Triggered from admin -> links import page. Runs on separate thread and send async message back to page via log() method
     *
     * @param online
     */
    def loadPreferredImages(online) {
        def updatedTaxa = []
        Integer batchSize = 20
        Map listConfig = this.getConfigFile(grailsApplication.config.imagesListsUrl) // config defined JSON file
        Map imagesMap = collectImageLists(listConfig.lists) // reads preferred images list via WS
        List guidList = []

        imagesMap.each { k, v ->
            guidList.add(v.taxonID)
        }

        int totalDocs = guidList.size()
        int totalPages = ((totalDocs + batchSize - 1) / batchSize) - 1
        def totalDocumentsUpdated = 0
        def buffer = []
        log "totalDocs = ${totalDocs} || totalPages = ${totalPages}"

        if (totalDocs > 0) {
            (0..totalPages).each { page ->
                def startInd = page * batchSize
                def endInd = (startInd + batchSize - 1) < totalDocs ? (startInd + batchSize - 1) : totalDocs - 1
                log "GUID batch = ${startInd} to ${endInd}"
                String guids = guidList[startInd..endInd].join("\",\"")
                updateProgressBar(totalPages, page)
                def paramsMap = [
                        q: "guid:\"" + guids +"\"",
                        rows: "${batchSize}",
                        wt: "json"
                ]
                MapSolrParams solrParams = new MapSolrParams(paramsMap)
                def searchResults = searchService.getCursorSearchResults(solrParams, !online)
                def resultsDocs = searchResults?.response?.docs?:[]
                log "SOLR query returned ${searchResults?.response?.numFound} docs"
                resultsDocs.each { Map doc ->
                    if (doc.containsKey("id") && doc.containsKey("guid") && doc.containsKey("idxtype")) {
                        //String imageId = getImageFromParamList(preferredImagesList, doc.guid)
                        def listEntry = imagesMap[doc.guid]
                        String imageId = listEntry?.imageId
                        if (!doc.containsKey("image") || (doc.containsKey("image") && doc.image != imageId)) {
                            Map updateDoc = [:]
                            updateDoc["id"] = doc.id // doc key
                            updateDoc["idxtype"] = ["set": doc.idxtype] // required field
                            updateDoc["guid"] = ["set": doc.guid] // required field
                            updateDoc["image"] = ["set": imageId]
                            updateDoc["imageAvailable"] = ["set": true]
                            log "Updated doc: ${doc.id} with imageId: ${imageId}"
                            totalDocumentsUpdated ++
                            buffer << updateDoc
                        }
                    } else {
                        log.warn "Updating doc error: missing keys ${doc}"
                    }
                }
            }

            if (buffer.size() > 0) {
                log("Committing ${totalDocumentsUpdated} docs to SOLR...")
                indexService.indexBatch(buffer, true)
                updatedTaxa = searchService.getTaxa(guidList)
            } else {
                log "No documents to update"
            }
        }

        updatedTaxa

    }

    /**
     * Denormalise the accepted taxa.
     * <p>
     * Do this by working down the hierarchy, Going from parent to child with a new set of parent taxa.
     * This is complicated if there are any accepted taxa with a dangling parent. We treat these as
     * root taxa, anyway.
     *
     * @param online Use the online index
     */
    def denormaliseTaxa(online) {
        int pageSize = BATCH_SIZE
        int bufferLimit = BUFFER_SIZE
        int processed = 0
        def js = new JsonSlurper()
        def baseUrl = online ? grailsApplication.config.indexLiveBaseUrl : grailsApplication.config.indexOfflineBaseUrl
        def prevCursor = ""
        def cursor = "*"
        def startTime, endTime
        def capitalisers = [:]
        Set autoLanguages = grailsApplication.config.autoComplete.languages ? grailsApplication.config.autoComplete.languages.split(',') as Set : null

        log("Starting dernomalisation")
        js.setType(JsonParserType.INDEX_OVERLAY)
        log("Getting species groups")
        def speciesGroupMapper = speciesGroupService.invertedSpeciesGroups
        log("Starting denormalisation scan for ${online ? 'online' : 'offline'} index")
        log("Clearing existing denormalisations")
        try {
            startTime = System.currentTimeMillis()

            def solrServerUrl = baseUrl + "/select?wt=json&q=denormalised_b:true&rows=1"
            def queryResponse = solrServerUrl.toURL().getText("UTF-8")
            def json = js.parseText(queryResponse)
            int total = json.response.numFound
            while (prevCursor != cursor) {
                solrServerUrl = baseUrl + "/select?wt=json&q=denormalised_b:true&cursorMark=${cursor}&sort=id+asc&rows=${pageSize}"
                queryResponse = solrServerUrl.toURL().getText("UTF-8")
                json = js.parseText(queryResponse)
                def docs = json.response.docs
                def buffer = []

                docs.each { doc ->
                    def update = [:]
                    update["id"] = doc.id // doc key
                    update["idxtype"] = [set: doc.idxtype] // required field
                    update["guid"] = [set: doc.guid] // required field
                    update["denormalised_b"] = [set: false ]
                    doc.each { k, v -> if (k.startsWith("rk_") || k.startsWith("rkid_")) update[k] = [set: null] }
                    doc.each { k, v -> if (k.startsWith("commonName")) update[k] = [set: null] }
                    update["nameVariant"] = [set: null]
                    update["commonName"] = [set: null]
                    update["commonNameExact"] = [set: null]
                    update["commonNameSingle"] = [set: null]
                    update["speciesGroup"] = [set: null]
                    update["speciesSubgroup"] = [set: null]
                    processed++
                    buffer << update
                }
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                if (total > 0) {
                    def percentage = Math.round((processed / total) * 100 )
                    log("Cleared ${processed} taxa (${percentage}%)")
                }
                prevCursor = cursor
                cursor = json.nextCursorMark
            }
        } catch (Exception ex) {
            log("Exception clearing denormalisation entries: ${ex.getMessage()}")
            log.error("Unable to clear denormalisations", ex)
        }
        log("Denormalising top-level taxa")
        try {
            processed = 0
            prevCursor = ""
            cursor = "*"
            def typeQuery = "idxtype:\"" + IndexDocType.TAXON.name() + "\"+AND+-acceptedConceptID:*+AND+-parentGuid:*"
            def solrServerUrl = baseUrl + "/select?wt=json&q=${typeQuery}&rows=1"
            def queryResponse = solrServerUrl.toURL().getText("UTF-8")
            def json = js.parseText(queryResponse)
            int total = json.response.numFound
            while (prevCursor != cursor) {
                //startTime = System.currentTimeMillis()
                solrServerUrl = baseUrl + "/select?wt=json&q=${typeQuery}&cursorMark=${cursor}&sort=id+asc&rows=${pageSize}"
                queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                json = js.parseText(queryResponse)
                def docs = json.response.docs
                def buffer = []
                log "1. Paging over ${total} docs - page ${(processed + 1)}"

                docs.each { doc ->
                    denormaliseEntry(doc, [:], [], [], [], buffer, bufferLimit, pageSize, online, js, speciesGroupMapper, autoLanguages, capitalisers)
                }
                processed++
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                if (total > 0 && processed % BUFFER_SIZE == 0) {
                    def percentage = Math.round(processed * 100 / total)
                    log("Denormalised ${processed} top-level taxa (${percentage}%)")
                }
                prevCursor = cursor
                cursor = json.nextCursorMark
            }
        } catch (Exception ex) {
            log("Exception denormalising top-level: ${ex.getMessage()}")
            log.error("Unable to denormalise", ex)
        }
        log("Denormalising dangling taxa")
        try {
            processed++
            prevCursor = ""
            cursor = "*"
            def danglingQuery = "idxtype:\"${IndexDocType.TAXON.name()}\"+AND++-acceptedConceptID:*+AND+-denormalised_s:yes"
            def solrServerUrl = baseUrl + "/select?wt=json&q=${danglingQuery}&rows=1"
            def queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
            def json = js.parseText(queryResponse)
            int total = json.response.numFound
            while (prevCursor != cursor) {
                //startTime = System.currentTimeMillis()
                solrServerUrl = baseUrl + "/select?wt=json&q=${danglingQuery}&cursorMark=${cursor}&sort=id+asc&rows=${pageSize}"
                queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                json = js.parseText(queryResponse)
                def docs = json.response.docs
                def buffer = []
                log "2. Paging over ${total} docs - page ${(processed + 1)}"

                docs.each { doc ->
                    denormaliseEntry(doc, [:], [], [], [], buffer, bufferLimit, pageSize, online, js, speciesGroupMapper, autoLanguages, capitalisers)
                }
                processed++
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                if (total > 0 && processed % BUFFER_SIZE == 0) {
                    def percentage = Math.round(processed * 100 / total)
                    log("Denormalised ${processed} dangling taxa (${percentage}%)")
                }
                prevCursor = cursor
                cursor = json.nextCursorMark
            }
        } catch (Exception ex) {
            log("Exception denormalising dangling taxa: ${ex.getMessage()}")
            log.error("Unable to denormalise", ex)
        }
        log("Denormalising synonyms")
        try {
            processed = 0
            prevCursor = ""
            cursor = "*"
            def synonymQuery = "idxtype:\"${IndexDocType.TAXON.name()}\"+AND+acceptedConceptID:*"
            def solrServerUrl = baseUrl + "/select?wt=json&q=${synonymQuery}&rows=1"
            def queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
            def json = js.parseText(queryResponse)
            int total = json.response.numFound
            while (prevCursor != cursor) {
                //startTime = System.currentTimeMillis()
                solrServerUrl = baseUrl + "/select?wt=json&q=${synonymQuery}&cursorMark=${cursor}&sort=id+asc&rows=${pageSize}"
                queryResponse = Encoder.encodeUrl(solrServerUrl).toURL().getText("UTF-8")
                json = js.parseText(queryResponse)
                def docs = json.response.docs
                def buffer = []
                log "3. Paging over ${total} docs - page ${(processed + 1)}"

                docs.each { doc ->
                    def accepted = searchService.lookupTaxon(doc.acceptedConceptID, !online)
                    if (accepted) {
                        def update = [:]
                        update["id"] = doc.id // doc key
                        update["idxtype"] = [set: doc.idxtype] // required field
                        update["guid"] = [set: doc.guid ] // required field
                        update["acceptedConceptName"] = [set: accepted.nameComplete ?: accepted.scientificName ]

                        buffer << update
                    }
                    processed++
                     if (buffer.size() >= bufferLimit) {
                        indexService.indexBatch(buffer, online)
                        buffer.clear()
                    }
                    if (total > 0 && processed % BUFFER_SIZE == 0) {
                        def percentage = Math.round(processed * 100 / total)
                        log("Denormalised ${processed} synonyms (${percentage}%)")
                    }
                }
                if (!buffer.isEmpty())
                    indexService.indexBatch(buffer, online)
                prevCursor = cursor
                cursor = json.nextCursorMark
            }
        } catch (Exception ex) {
            log("Exception denormalising dangling taxa: ${ex.getMessage()}")
            log.error("Unable to denormalise", ex)
        }
        endTime = System.currentTimeMillis()
        log("Finished taxon denormalisaion. Duration: ${(new SimpleDateFormat("mm:ss:SSS")).format(new Date(endTime - startTime))}")
    }

    private denormaliseEntry(doc, Map trace, List stack, List speciesGroups, List speciesSubGroups, List buffer, int bufferLimit, int pageSize, boolean online, JsonSlurper js, Map speciesGroupMapping, Set autoLanguages, Map capitalisers) {
        def currentDistribution = (doc['distribution'] ?: []) as Set
        if (doc.denormalised_b)
            return currentDistribution
        def baseUrl = online ? grailsApplication.config.indexLiveBaseUrl : grailsApplication.config.indexOfflineBaseUrl
        def update = [:]
        def distribution = [] as Set
        def guid = doc.guid
        def scientificName = doc.scientificName
        def nameComplete = doc.nameComplete
        if (stack.contains(guid)) {
            log "Loop in parent-child relationship for ${guid} - ${stack}"
            return currentDistribution
        }
        stack << guid
        update["id"] = doc.id // doc key
        update["idxtype"] = [set: doc.idxtype] // required field
        update["guid"] = [set: guid] // required field
        update["denormalised_b"] = [set: true ]
        update << trace

        if (doc.rank && doc.rankID && doc.rankID != 0) {
            def normalisedRank = normaliseRank(doc.rank)
            trace = trace.clone()
            trace << [("rk_" + normalisedRank): [set: scientificName]]
            trace << [("rkid_" + normalisedRank): [set: doc.guid]]
            // we have a unique rank name and value, check if it's in the species group list
            def rn = new RankedName(name: scientificName.toLowerCase(), rank: normalisedRank)
            def speciesGroup = speciesGroupMapping[rn]
            if (speciesGroup) {
                log("Adding group ${speciesGroup.group} and subgroup ${speciesGroup.subGroup} to $scientificName")
                speciesGroups = speciesGroups.clone()
                speciesGroups << speciesGroup.group
                speciesSubGroups = speciesSubGroups.clone()
                speciesSubGroups << speciesGroup.subGroup
            }
            update["speciesGroup"] = [set: speciesGroups]
            update["speciesSubgroup"] = [set: speciesSubGroups]
        }
        def commonNames = searchService.lookupVernacular(guid, !online)
        if (commonNames && !commonNames.isEmpty()) {
            commonNames = commonNames.sort { n1, n2 ->
                n2.priority - n1.priority
            }

            //only index valid languages
            if (autoLanguages)
                commonNames = commonNames.findAll { autoLanguages.contains(it.language) }

            def names = new LinkedHashSet(commonNames.collect { it.name })
            if(commonNames) {
                update["commonName"] = [set: names]
                update["commonNameExact"] = [set: names]
                update["commonNameSingle"] = [set: names.first() ]
            }
        }
        def identifiers = searchService.lookupIdentifier(guid, !online)
        if (identifiers) {
            update["additionalIdentifiers"] = [set: identifiers.collect { it.guid }]
        }
        def variants = searchService.lookupVariant(guid, !online)
        if (variants) {
            float min = grailsApplication.config.getProperty("priority.min", Float, 0.25)
            float max = grailsApplication.config.getProperty("priority.max", Float, 5.0)
            float norm = grailsApplication.config.getProperty("priority.norm", Float, 4000.0)
            float boost = variants.collect({
                def priority = it.priority ?: norm
                Math.min(max, Math.max(min, priority / norm))
            }).max()
            def names = (variants.collect { it.scientificName }) as Set
            names.addAll(variants.collect { it.nameComplete })
            names.remove(null)
            names.remove(scientificName)
            names.remove(nameComplete)
            if (names)
                update["nameVariant"] = [boost: boost, set: names]
            update["scientificName"] = [boost: boost, set: scientificName]
            if (nameComplete)
                update["nameComplete"] = [boost: boost, set: nameComplete]
        }

        def prevCursor = ""
        def cursor = "*"
        while (cursor != prevCursor) {
            def encGuid = UriUtils.encodeQueryParam(doc.guid, 'UTF-8')
            def parentQuery = "idxtype:%22${IndexDocType.TAXON.name()}%22+AND+taxonomicStatus:accepted+AND+parentGuid:%22${encGuid}%22"
            def solrServerUrl = baseUrl + "/select?wt=json&q=${parentQuery}&cursorMark=${cursor}&sort=id+asc&rows=${pageSize}"
            def queryResponse = solrServerUrl.toURL().getText("UTF-8")
            def json = js.parseText(queryResponse)
            def docs = json.response.docs
            docs.each { child ->
                distribution.addAll(denormaliseEntry(child, trace, stack, speciesGroups, speciesSubGroups, buffer, bufferLimit, pageSize, online, js, speciesGroupMapping, autoLanguages, capitalisers))
            }
            prevCursor = cursor
            cursor = json.nextCursorMark
        }
        def additionalDistribtion = distribution.findAll { !currentDistribution.contains(it) }
        if (!additionalDistribtion.isEmpty())
            update['distribution'] = [add: additionalDistribtion]
        buffer << update

        if (buffer.size() >= bufferLimit) {
            indexService.indexBatch(buffer, online)
            buffer.clear()
        }
        stack.pop()
        distribution.addAll(currentDistribution)
        return distribution
    }

    /**
     * Get the taxon rank structure
     *
     * @return
     */
    def ranks() {
        JsonSlurper slurper = new JsonSlurper()
        def ranks = slurper.parse(this.class.getResource("/taxonRanks.json"))
        def idMap = [:]
        def iter = ranks.iterator()
        while (iter.hasNext()) {
            def entry = iter.next()
            idMap.put(entry.rank, entry)
        }
        idMap
    }

    /**
     * Get vernacular name information
     */
    def vernacularNameStatus() {
        JsonSlurper slurper = new JsonSlurper()
        def ranks = slurper.parse(this.class.getResource("/vernacularNameStatus.json"))
        def idMap = [:]
        def iter = ranks.iterator()
        while (iter.hasNext()) {
            def entry = iter.next()
            idMap.put(entry.status, entry)
        }
        idMap
    }

    /**
     * Get identifier information
     */
    def identifierStatus() {
        JsonSlurper slurper = new JsonSlurper()
        def ranks = slurper.parse(this.class.getResource("/identifierStatus.json"))
        def idMap = [:]
        def iter = ranks.iterator()
        while (iter.hasNext()) {
            def entry = iter.next()
            idMap.put(entry.status, entry)
        }
        idMap
    }

    /**
     * Build a complete name + author
     * <p>
     * Some names are funny. So if there is a name supplied used that.
     * Otherwise try to build the name from scientific name + authorship
     *
     * @param nameComplete The supplied complete name, if available
     * @param scientificName The scientific name
     * @param scientificNameAuthorship The authorship
     * @return
     */
    String buildNameComplete(String nameComplete, String scientificName, String scientificNameAuthorship) {
        if (nameComplete)
            return nameComplete
        if (scientificNameAuthorship)
            return scientificName + " " + scientificNameAuthorship
        return scientificName
    }

    /**
     * Build an HTML formatted name
     * <p>
     * If a properly formatted name is supplied, then use that.
     * Otherwise, try yo build the name from the supplied information.
     * The HTMLised name is escaped and uses spans to encode formatting information.
     *
     *
     * @param nameFormatted The formatted name, if available
     * @param nameComplete The complete name, if available
     * @param scientificName The scientific name
     * @param scientificNameAuthorship The name authorship
     * @param rank The taxon rank
     * @param rankMap The lookup table for ranks
     *
     * @return The formatted name
     */
    String buildNameFormatted(String nameFormatted, String nameComplete, String scientificName, String scientificNameAuthorship, String rank, Map rankMap) {
        def rankGroup = rankMap.get(rank)?.rankGroup ?: "unknown"
        def formattedCssClass = rank ? "scientific-name rank-${rankGroup}" : "scientific-name";

        if (nameFormatted)
            return nameFormatted
        if (nameComplete) {
            def authorIndex = scientificNameAuthorship ? nameComplete.indexOf(scientificNameAuthorship) : -1
            if (authorIndex <= 0)
                return "<span class=\"${formattedCssClass}\">${StringEscapeUtils.escapeHtml(nameComplete)}</span>"
            def preAuthor = nameComplete.substring(0, authorIndex).trim()
            def postAuthor = nameComplete.substring(authorIndex + scientificNameAuthorship.length()).trim()
            def name = "<span class=\"${formattedCssClass}\">"
            if (preAuthor && !preAuthor.isEmpty())
                name = name + "<span class=\"name\">${StringEscapeUtils.escapeHtml(preAuthor)}</span> "
            name = name + "<span class=\"author\">${StringEscapeUtils.escapeHtml(scientificNameAuthorship)}</span>"
            if (postAuthor && !postAuthor.isEmpty())
                name = name + " <span class=\"name\">${StringEscapeUtils.escapeHtml(postAuthor)}</span>"
            name = name + "</span>"
            return name
        }
        if (scientificNameAuthorship)
            return "<span class=\"${formattedCssClass}\"><span class=\"name\">${StringEscapeUtils.escapeHtml(scientificName)}</span> <span class=\"author\">${StringEscapeUtils.escapeHtml(scientificNameAuthorship)}</span></span>"
        return "<span class=\"${formattedCssClass}\"><span class=\"name\">${StringEscapeUtils.escapeHtml(scientificName)}</span></span>"
    }


    /**
     * Helper method to do a HTTP GET and return String content
     *
     * @param url
     * @return
     */
    private String getStringForUrl(String url) throws IOException {
        String output = ""
        def inStm = new URL(Encoder.encodeUrl(url)).openStream()
        try {
            output = IOUtils.toString(inStm)
        } finally {
            IOUtils.closeQuietly(inStm)
        }
        output
    }

    def log(msg) {
        log.info(msg)
        brokerMessagingTemplate.convertAndSend "/topic/import-feedback", msg.toString()
    }

    private updateProgressBar(int total, int current) {
        Double percentDone = total > 0 ? current * 100.0 / total : 100.0
        brokerMessagingTemplate.convertAndSend "/topic/import-progress", percentDone.round(1).toString()
    }

    private updateProgressBar2(int total, int current) {
        Double percentDone = total > 0 ? current * 100.0 / total : 100.0
        brokerMessagingTemplate.convertAndSend "/topic/import-progress2", percentDone.round(1).toString()
    }

    private findAttribution(datasetID, attributionMap, datasetMap) {
        if (attributionMap.containsKey(datasetID))
            return attributionMap.get(datasetID)
        def attribution = null
        def dataset = searchService.getDataset(datasetID, datasetMap, true)
        if (dataset && dataset["name"]) {
            attribution = [datasetName: dataset["name"]]
        }
        attributionMap.put(datasetID, attribution)
        return attribution
    }

    private getConfigFile(String url) {
        //url = URLEncoder.encode(url, "UTF-8")
        URL source = this.class.getResource(url)
        if (source == null)
            source = new URL(url)
        JsonSlurper slurper = new JsonSlurper()
        return slurper.parse(source)
     }
}