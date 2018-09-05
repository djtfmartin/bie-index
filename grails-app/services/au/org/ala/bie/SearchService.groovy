package au.org.ala.bie

import au.org.ala.bie.search.IndexDocType
import au.org.ala.bie.util.Encoder
import grails.converters.JSON
import grails.web.servlet.mvc.GrailsParameterMap
import groovy.json.JsonSlurper
import org.apache.solr.common.params.MapSolrParams
import org.gbif.nameparser.PhraseNameParser
import org.springframework.web.util.UriUtils

/**
 * A set of search services for the BIE.
 */
class SearchService {

    static BULK_BATCH_SIZE = 20

    def grailsApplication
    def conservationListsSource

    def additionalResultFields = null

    /**
     * Retrieve species & subspecies for the supplied taxon which have images.
     *
     * @param taxonID
     * @param start
     * @param rows
     * @return
     */
    def imageSearch(taxonID, start, rows, queryContext){

        def query = "q=*:*"

        if(taxonID){
            //retrieve the taxon rank and then construct the query
            def taxon = lookupTaxon(taxonID)
            if(!taxon){
                return []
            }
            def tid = taxon.guid // no longer encoded here
            query = "q=(guid:\"${tid}\"+OR+rkid_${taxon.rank.toLowerCase().replaceAll('\\s', '_')}:\"${tid}\")"
        }

        def additionalParams = "&wt=json&fq=rankID:[7000 TO *]&fq=imageAvailable:true"

        if(start){
            additionalParams = additionalParams + "&start=" + start
        }

        if(rows){
            additionalParams = additionalParams + "&rows=" + rows
        }

        if(queryContext){
            additionalParams = additionalParams + queryContext
        }

        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?" + query + additionalParams

        log.debug(queryUrl)
        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")

        def js = new JsonSlurper()

        def json = js.parseText(queryResponse)
        log.debug "imageSearch response json = ${json}"

        [
                totalRecords:json.response.numFound,
                facetResults: formatFacets(json.facet_counts?.facet_fields?:[:]),
                results: formatDocs(json.response.docs, null, null)
        ]
    }

    /**
     * Retrieve species & subspecies for the supplied taxon which have images.
     *
     * @param taxonID
     * @param start
     * @param rows
     * @return
     */
    def imageLinkSearch(taxonID, type, queryContext){
        def result = imageSearch(taxonID, 0, 1, queryContext)
        if (!result || result.isEmpty() || result.totalRecords == 0) {
            return null
        }
        def taxon = result.results.get(0)
        if (!taxon.image || taxon.image.isEmpty()) {
            return null
        }
        if (type == 'thumbnail') {
            return grailsApplication.config.imageThumbnailUrl + taxon.image
        } else if (type == 'small') {
            return grailsApplication.config.imageSmallUrl + taxon.image
        } else if (type == 'large') {
            return grailsApplication.config.imageLargeUrl + taxon.image
        } else {
            return grailsApplication.config.imageLargeUrl + taxon.image
        }
    }


    /**
     * General search service.
     *
     * @param requestedFacets
     * @return
     */
    def search(String q, GrailsParameterMap params, List requestedFacets) {
        params.remove("controller") // remove Grails stuff from query
        params.remove("action") // remove Grails stuff from query
        log.debug "params = ${params.toMapString()}"
        def fqs = params.fq
        def query = []
        def queryTitle = null


        if (q) {
            queryTitle = q
            if (!q) {
                q = q.replaceFirst("q=", "q=*:*")
            } else if (q.trim() == "*") {
                q = q.replaceFirst("q=*", "q=*:*")
            }
            // boost query syntax was removed from here. NdR.

            // Add fuzzy search term modifier to simple queries with > 1 term (e.g. no braces)
            if (!q.contains("(") && q.trim() =~ /\s+/) {
                q = q.replaceAll('"', " ").trim()
                def queryArray = []
                q.split(/\s+/).each {
                    if (!(it =~ /AND|OR|NOT/)) {
                        queryArray.add(it + "~0.8")
                    } else {
                        queryArray.add(it)
                    }
                }
                def nq = queryArray.join(" ")
                log.debug "fuzzy nq = ${nq}"
                q = "\"${q}\"^100 ${nq}"
            }
        } else {
            q = "*:*"
            queryTitle = "all records"
        }
        query << "q=${q}"

        // Add query parameters
        query << "defType=${grailsApplication.config.solr.defType}" // Query parser type
        query << "qf=${grailsApplication.config.solr.qf}" // dismax query fields
        if ([Collection, Object[]].any { it.isAssignableFrom(grailsApplication.config.solr.bq.getClass())}) { //array, as per default config
            grailsApplication.config.solr.bq.each { query << "bq=${it}" } // dismax boosts
        } else {
            query << "${grailsApplication.config.solr.bq}" //string, as per current ansible scripts
        }
        query << "q.alt=${grailsApplication.config.solr.qAlt}" // if no query specified use this query
        if ([Collection, Object[]].any { it.isAssignableFrom(grailsApplication.config.solr.hl.getClass())}) { //array, as per default config
            grailsApplication.config.solr.hl.each { query << "hl=${it}" } // highlighting parameters
        } else {
            query << "hl=${grailsApplication.config.solr.hl}" //string, as per current ansible scripts
        }
        query << "wt=json"
        query << "facet=${!requestedFacets.isEmpty()}"
        query << "facet.mincount=1"

        if (requestedFacets) {
            requestedFacets.each { query << "facet.field=${it}" }
        }

        //pagination params
        query << "start=${params.start ?: 0}"
        query << "rows=${params.rows ?: params.pageSize ?: 10}"

        if (params.sort) {
            query << "sort=${params.sort} ${params.dir ?: 'asc'}" // sort dir example "&sort=name asc"
        }

        grailsApplication.config.solr.fq.each { query << "&fq=${it}"}
        if(fqs){
            if(isCollectionOrArray(fqs)){
                fqs.each { query << "&fq=${it}" }
            } else {
                query << "&fq=${fqs}"
            }
        }

        String solrUlr = grailsApplication.config.indexLiveBaseUrl + "/select?" + query.join('&')
        log.debug "SOLR URL = ${solrUlr}"
        def queryResponse = new URL(Encoder.encodeUrl(solrUlr)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        if (json.response.numFound as Integer == 0) {

            try {

                //attempt to parse the name
                def nameParser = new PhraseNameParser()
                def parsedName = nameParser.parse(q)
                if (parsedName && parsedName.canonicalName()) {
                    def canonical = parsedName.canonicalName()
                    // TODO test if this breaks paginating through results... looks like it will
                    def sciNameQuery = grailsApplication.config.indexLiveBaseUrl + "/select?q=scientificName:\"" + canonical + "\"" + additionalParams
                    log.debug "sciNameQuery = ${sciNameQuery}"
                    queryResponse = new URL(Encoder.encodeUrl(sciNameQuery)).getText("UTF-8")
                    js = new JsonSlurper()
                    json = js.parseText(queryResponse)
                }
            } catch(Exception e){
                //expected behaviour for non scientific name matches
                log.debug "expected behaviour for non scientific name matches: ${e}"
            }
        }

        def matcher = ( queryTitle =~ /(rkid_)([a-z]{1,})(:)(.*)/ )
        if(matcher.matches()){
            try {
                def rankName = matcher[0][2]
                def guid = matcher[0][4]
                def shortProfile = getShortProfile(guid)
                if (shortProfile)
                    queryTitle = rankName + " " + shortProfile.scientificName
                else
                    queryTitle = rankName + " " + guid
            } catch (Exception e){
                log.warn("Exception thrown parsing name..", e)
            }
        }

        if(!queryTitle){
            queryTitle = "all records"
        }

        log.debug("search called with q = ${q}, returning ${json.response.numFound}")

        [
            totalRecords: json.response.numFound,
            facetResults: formatFacets(json.facet_counts?.facet_fields ?: [:], requestedFacets),
            results     : formatDocs(json.response.docs, json.highlighting, params),
            queryTitle  : queryTitle
        ]
    }

    boolean isCollectionOrArray(object) {
        [Collection, Object[]].any { it.isAssignableFrom(object.getClass()) }
    }

    def getHabitats(){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1000&q=idxtype:" + IndexDocType.HABITAT.toString()
        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        def children = []
        def taxa = json.response.docs
        taxa.each { taxon ->
            children << [
                    guid:taxon.guid,
                    parentGuid: taxon.parentGuid,
                    name: taxon.name
            ]
        }
        children
    }

    def getHabitatsIDsByGuid(guid){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1&q=idxtype:" + IndexDocType.HABITAT.toString() +
                "&fq=guid:\"" + guid + "\""

        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        //construct a tree
        def ids = []
        if(json.response.docs){
            def doc = json.response.docs[0]
            ids << doc.name
            ids << getChildHabitatIDs(doc.guid)
        }

        ids.flatten()
    }

    private def getChildHabitatIDs(guid){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1000&q=idxtype:" + IndexDocType.HABITAT.toString() +
                "&fq=parentGuid:\"" + guid + "\""

        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        def ids = []
        //construct a tree
        json.response.docs.each {
            ids << it.name
            ids << getChildHabitatIDs(it.guid)
        }
        ids
    }

    def getHabitatByGuid(guid){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1&q=idxtype:" + IndexDocType.HABITAT.toString() +
                "&fq=guid:\"" + guid + "\""

        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        //construct a tree
        def root = [:]
        if(json.response.docs){
            def doc = json.response.docs[0]
            return [
                    guid:doc.guid,
                    name: doc.name,
                    children: getChildHabitats(doc.guid)
            ]
        }
    }

    private def getChildHabitats(guid){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1000&q=idxtype:" + IndexDocType.HABITAT.toString() +
                "&fq=parentGuid:\"" + guid + "\""

        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        def subTree = []
        //construct a tree
        json.response.docs.each {
            subTree << [
                    guid:it.guid,
                    name: it.name,
                    children: getChildHabitats(it.guid)
            ]
        }
        subTree
    }

    def getHabitatsTree(){
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1000&q=idxtype:" + IndexDocType.HABITAT.toString()
        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        //construct a tree
        def root = [:]
        json.response.docs.each {
            if(!it.parentGuid){
                root[it.guid] = [
                    guid:it.guid,
                    name: it.name
                ]
            }
        }
        //look for children of the root
        def nodes = root.values()
        nodes.each { addChildren(json.response.docs, it) }
        root
    }

    private def addChildren(docs, node){
        docs.each {
            if(it.parentGuid && node.guid == it.parentGuid){
                if(!node.children){
                    node.children = [:]
                }
                def childNode = [
                        guid:it.guid,
                        name: it.name
                ]

                node.children[it.guid] = childNode
                addChildren(docs, childNode)
            }
        }
    }


    def getChildConcepts(taxonID, queryString){
        def encID = taxonID // URLEncoder.encode(taxonID, "UTF-8")
        def queryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&rows=1000&q=parentGuid:\"" + encID + "\""

        if(queryString){
            queryUrl = queryUrl + "&" + queryString
        }

        def queryResponse = new URL(Encoder.encodeUrl(queryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        def children = []
        def taxa = json.response.docs
        taxa.each { taxon ->
            children << [
                    guid:taxon.guid,
                    parentGuid: taxon.parentGuid,
                    name: taxon.scientificName,
                    nameComplete: taxon.nameComplete ?: taxon.scientificName,
                    nameFormatted: taxon.nameFormatted,
                    author: taxon.scientificNameAuthorship,
                    rank: taxon.rank,
                    rankID:taxon.rankID
            ]
        }
        children.sort { it.name }
    }

    /**
     * Retrieve details of a taxon by taxonID
     *
     * @param taxonID
     * @param useOfflineIndex
     * @return
     */
    def lookupTaxon(String taxonID, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = grailsApplication.config.indexLiveBaseUrl

        if (useOfflineIndex) {
            indexServerUrlPrefix = grailsApplication.config.indexOfflineBaseUrl
        }

        def encID = taxonID // encoding now done by Encoder.encode()
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=guid:\"" + encID + "\"+OR+linkIdentifier:\"" + encID + "\"&fq=idxtype:" + IndexDocType.TAXON.name()
        def queryResponse = new URL(Encoder.encodeUrl(indexServerUrl)).getText("UTF-8")
        log.debug "lookupTaxon url = ${Encoder.encodeUrl(indexServerUrl)}"
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        log.debug "lookupTaxon response = ${json}"
        json.response.docs[0]
    }

    /**
     * Retrieve details of a taxon by common name or scientific name
     * @param taxonID
     * @return
     */
    private def lookupTaxonByName(String taxonName, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = grailsApplication.config.indexLiveBaseUrl
        if (useOfflineIndex) {
            indexServerUrlPrefix = grailsApplication.config.indexOfflineBaseUrl
        }
        def solrServerUrl = indexServerUrlPrefix + "/select?wt=json&q=" +
                "commonNameExact:\"" + taxonName + "\" OR scientificName:\"" + taxonName + "\" OR exact_text:\"" + taxonName + "\"" + // exact_text added to handle case differences in query vs index
                (grailsApplication.config?.solr?.bq ? "&" + grailsApplication.config.solr.bq + "&defType=dismax" : "") //use boosting if provided, since the first match will be selected which is otherwise arbitrary
        
        def queryResponse = new URL(Encoder.encodeUrl(solrServerUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs[0]
    }

    /**
     * Request is for an old identifier - lookup current taxon
     *
     * @param identifier
     * @param useOfflineIndex
     * @return
     * @throws Exception
     */
    private def lookupTaxonByPreviousIdentifier(String identifier, Boolean useOfflineIndex = false) throws Exception {
        def indexServerUrlPrefix = grailsApplication.config.indexLiveBaseUrl
        if (useOfflineIndex) {
            indexServerUrlPrefix = grailsApplication.config.indexOfflineBaseUrl
        }
        String solrServerUrl = indexServerUrlPrefix + "/select?wt=json&q=guid:%22" + UriUtils.encodeQueryParam(identifier, 'UTF-8') +"%22"
        log.debug "SOLR url = ${solrServerUrl}"
        def queryResponse = new URL(solrServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)

        def taxonGuid = ""
        def taxon = null

        if (json.response?.docs) {
            taxonGuid = json.response.docs[0].taxonGuid
        }

        if (taxonGuid) {
            taxon = lookupTaxon(taxonGuid, useOfflineIndex)
        }

        taxon
    }

    /**
     * Retrieve details of a specific vernacular name by taxonID
     *
     * @param taxonID The taxon identifier
     * @param name The vernacular name
     * @param useOfflineIndex
     * @return
     */
    def lookupVernacular(String taxonID, String vernacularName, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = useOfflineIndex ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl
        def encID = URLEncoder.encode(taxonID, 'UTF-8')
        def encName = URLEncoder.encode(vernacularName, "UTF-8")
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=taxonGuid:%22${encID}%22&fq=(idxtype:${IndexDocType.COMMON.name()}+AND+name:%22${encName}%22)"
        def queryResponse = new URL(indexServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs[0]
    }

    /**
     * Retrieve details of a specific identifier by taxonID
     *
     * @param taxonID The taxon identifier
     * @param identifier The identifier
     * @param useOfflineIndex
     * @return
     */
    def lookupIdentifier(String taxonID, String identifier, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = useOfflineIndex ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl
        def encID = UriUtils.encodeQueryParam(taxonID, 'UTF-8')
        def encIdentifier = UriUtils.encodeQueryParam(identifier, 'UTF-8') // URLEncoder.encode(identifier, "UTF-8")
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=taxonGuid:%22${encID}%22&fq=(idxtype:${IndexDocType.IDENTIFIER.name()}+AND+guid:\"${encIdentifier}\")"
        def queryResponse = new URL(indexServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs[0]
    }

    /**
     * Retrieve details of all vernacular names attached to a taxon.
     *
     * @param taxonID The taxon identifier
     * @param useOfflineIndex
     * @return
     */
    def lookupVernacular(String taxonID, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = useOfflineIndex ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl
        def encID = UriUtils.encodeQueryParam(taxonID, 'UTF-8')
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=taxonGuid:%22${encID}%22&fq=idxtype:${IndexDocType.COMMON.name()}"
        def queryResponse = new URL(indexServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs
    }

    /**
     * Retrieve details of all identifiers attached to a taxon.
     *
     * @param taxonID The taxon identifier
     * @param useOfflineIndex
     * @return
     */
    def lookupIdentifier(String taxonID, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = useOfflineIndex ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl
        def encID = UriUtils.encodeQueryParam(taxonID, 'UTF-8') // URLEncoder.encode(taxonID, 'UTF-8')
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=taxonGuid:%22${encID}%22&fq=idxtype:${IndexDocType.IDENTIFIER.name()}"
        def queryResponse = new URL(indexServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs
    }
    
    /**
     * Retrieve details of all name vairants attached to a taxon.
     *
     * @param taxonID The taxon identifier
     * @param useOfflineIndex
     * @return
     */
    def lookupVariant(String taxonID, Boolean useOfflineIndex = false){
        def indexServerUrlPrefix = useOfflineIndex ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl
        def encID = UriUtils.encodeQueryParam(taxonID, 'UTF-8') // URLEncoder.encode(taxonID, 'UTF-8')
        def indexServerUrl = indexServerUrlPrefix+ "/select?wt=json&q=taxonGuid:%22${encID}%22&fq=idxtype:${IndexDocType.TAXONVARIANT.name()}"
        def queryResponse = new URL(indexServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs
    }

    /**
     * Return a simplified profile object for the docs that match the provided name
     *
     * @param name
     * @return Map with 4 fields
     */
    def getProfileForName(String name){
        String qf = "qf=scientificName^200+commonName^50+exact_text^100"
        String bq = "bq=taxonomicStatus:accepted^1000&bq=rankID:7000^500&bq=rankID:6000^100&bq=-scientificName:\"*+x+*\"^100"
        String additionalParams = "&defType=edismax&${qf}&${bq}&wt=json".toString()
        def queryString = "q=" + "\"" + name + "\"" + "&fq=idxtype:" + IndexDocType.TAXON.name()
        String url = grailsApplication.config.indexLiveBaseUrl + "/select?" + queryString + additionalParams
        log.debug "profile search for url: ${url}"
        def queryResponse = new URL(Encoder.encodeUrl(url)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        log.debug "getProfileForName - json = ${json}"
        def model = []

        if (json.response.numFound > 0) {
            json.response.docs.each { result ->
                model << [
                    "identifier": result.guid,
                    "name": result.scientificName,
                    "acceptedIdentifier": result.acceptedConceptID ?: (result.taxonomicStatus == "accepted" ? result.guid : ""),
                    "acceptedName": result.acceptedConceptName ?: (result.taxonomicStatus == "accepted" ? result.scientificName : "")
                ]
            }
        }

        model
    }

    Map getLongProfileForName(String name){
        String qf = "qf=scientificName^200+commonName^50+exact_text^100+doc_name"
        String bq = "bq=taxonomicStatus:accepted^1000&bq=rankID:7000^500&bq=rankID:6000^100&bq=-scientificName:\"*+x+*\"^100"
        def additionalParams = "&defType=edismax&${qf}&${bq}&wt=json"
        def queryString = "&q=" + "\"" + name + "\"" + "&fq=idxtype:" + IndexDocType.TAXON.name()
        log.debug "profile search for query: ${queryString}"
        String url = grailsApplication.config.indexLiveBaseUrl + "/select?" + queryString + additionalParams
        log.debug "profile searcURL: ${url}"
        def queryResponse = new URL(Encoder.encodeUrl(url)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        def model = [:]

        if (json.response.numFound > 0) {
            def result = json.response.docs[0]
            //json.response.docs.each { result ->
                model = [
                        "identifier": result.guid,
                        "guid": result.guid,
                        "parentGuid": result.parentGuid,
                        "name": result.scientificName,
                        "nameComplete": result.nameComplete,
                        "commonName" : result.commonName,
                        "commonNameSingle" : result.commonNameSingle,
                        "rank" : result.rank,
                        "rankId" : result.rankID,
                        "acceptedConceptGuid": result.acceptedConceptID ?: result.guid,
                        "acceptedConceptName": result.acceptedConceptName ?: result.scientificName,
                        "taxonomicStatus": result.taxonomicStatus,
                        "imageId": result.image,
                        "imageUrl": (result.image) ? grailsApplication.config.imageLargeUrl + result.image : "",
                        "thumbnailUrl": (result.image) ? grailsApplication.config.imageThumbnailUrl + result.image : "",
                        "largeImageUrl": (result.image) ? grailsApplication.config.imageSmallUrl + result.image : "",
                        "smallImageUrl": (result.image) ? grailsApplication.config.imageSmallUrl + result.image : "",
                        "imageMetadataUrl": (result.image) ? grailsApplication.config.imageMetaDataUrl + result.image : "",
                        "kingdom": result.rk_kingdom,
                        "phylum": result.rk_phylum,
                        "classs": result.rk_class,
                        "order":result.rk_order,
                        "family": result.rk_family,
                        "genus": result.rk_genus,
                        "author": result.scientificNameAuthorship,
                        "linkIdentifier": result.linkIdentifier
                ]

        }

        model
    }

    def getShortProfile(taxonID){
        log.debug "getShortProfile taxonID = ${taxonID}"
        def taxon = lookupTaxon(taxonID)
        if(!taxon){
            return null
        }
        def classification = extractClassification(taxon)
        def model = [
                taxonID:taxon.guid,
                scientificName: taxon.scientificName,
                scientificNameAuthorship: taxon.scientificNameAuthorship,
                author: taxon.scientificNameAuthorship,
                rank: taxon.rank,
                rankID:taxon.rankID,
                kingdom: classification.kingdom?:"",
                family: classification.family?:""
        ]

        if (taxon.commonNameSingle) {
            model.put("commonName",  taxon.commonNameSingle)
        } else if (taxon.commonName){
            model.put("commonName",  taxon.commonName.first())
        }

        if(taxon.image){
            model.put("thumbnail", grailsApplication.config.imageThumbnailUrl + taxon.image)
            model.put("imageURL", grailsApplication.config.imageLargeUrl + taxon.image)
        }
        model
    }

    def getTaxa(List guidList){
        def resultMap = [:]
        def matchingTaxa = []

        while (!guidList.isEmpty()) {
            def batch = guidList.take(BULK_BATCH_SIZE)
            def batchSet = (batch.findAll { !resultMap.containsKey(it) }) as Set
            def matches = getTaxaBatch(batchSet)
            if (!(matches instanceof List)) // Error return
                return matches
            matches.each { match ->
                resultMap[match.guid] = match
                if (match.linkIdentifier)
                    resultMap[match.linkIdentifier] = match
            }
            batch.each { guid ->
                matchingTaxa << resultMap[guid]
            }
            guidList = guidList.drop(BULK_BATCH_SIZE)
        }
        return matchingTaxa
    }

    private getTaxaBatch(Collection guidList) {
        if (!guidList)
            return []
        def queryList = guidList.collect({'"' + it + '"'}).join(',')
        def postBody = [
                q: "guid:(" + queryList + ") OR linkIdentifier:("  + queryList + ")",
                fq: "idxtype:" + IndexDocType.TAXON.name(),
                rows: BULK_BATCH_SIZE,
                wt: "json"
        ] // will be url-encoded
        def resp = doPostWithParams(grailsApplication.config.indexLiveBaseUrl +  "/select", postBody)

        //create the docs....
        if(resp?.resp?.response){

            def matchingTaxa = []

            resp.resp.response.docs.each { doc ->
               def taxon = [
                       guid: doc.guid,
                       name: doc.scientificName,
                       scientificName: doc.scientificName,
                       author: doc.scientificNameAuthorship,
                       nameComplete: doc.nameComplete?:doc.scientificName,
                       rank: doc.rank,
                       kingdom: doc.rk_kingdom,
                       phylum: doc.rk_phylum,
                       classs: doc.rk_class,
                       order: doc.rk_order,
                       family: doc.rk_family,
                       genus: doc.rk_genus,
                       datasetName: doc.datasetName,
                       datasetID: doc.datasetID
               ]
               if(doc.image){
                   taxon.put("thumbnailUrl", grailsApplication.config.imageThumbnailUrl + doc.image)
                   taxon.put("smallImageUrl", grailsApplication.config.imageSmallUrl + doc.image)
                   taxon.put("largeImageUrl", grailsApplication.config.imageLargeUrl + doc.image)
               }
                if (doc.linkIdentifier)
                    taxon.put("linkIdentifier", doc.linkIdentifier)
               if (doc.commonNameSingle) {
                   taxon.put("commonNameSingle", doc.commonNameSingle)
               } else if (doc.commonName) {
                   taxon.put("commonNameSingle", doc.commonName.first())
               }
               matchingTaxa << taxon
            }
            matchingTaxa
        } else {
            resp
        }
    }

    def getTaxon(taxonLookup){

        def taxon = lookupTaxon(taxonLookup)
        if(!taxon) {
            taxon = lookupTaxonByName(taxonLookup)
        }
        if(!taxon) {
            taxon = lookupTaxonByPreviousIdentifier(taxonLookup)
            if(!taxon){
                return null
            }
        }

        //retrieve any synonyms
        def synonymQueryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&q=" +
                "acceptedConceptID:\"" + taxon.guid + "\"" + "&fq=idxtype:" + IndexDocType.TAXON.name()
        def synonymQueryResponse = new URL(Encoder.encodeUrl(synonymQueryUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def synJson = js.parseText(synonymQueryResponse)

        def synonyms = synJson.response.docs

        def classification = extractClassification(taxon)

        //retrieve any common names
        def commonQueryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&q=" +
                "taxonGuid:\"" + taxon.guid + "\"" + "&fq=idxtype:" + IndexDocType.COMMON.name()
        def commonQueryResponse = new URL(Encoder.encodeUrl(commonQueryUrl)).getText("UTF-8")
        def commonJson = js.parseText(commonQueryResponse)
        def commonNames = commonJson.response.docs.sort { n1, n2 -> n2.priority - n1.priority }


        //retrieve any additional identifiers
        def identifierQueryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&q=" +
                "taxonGuid:\"" + taxon.guid + "\"" + "&fq=idxtype:" + IndexDocType.IDENTIFIER.name()
        def identifierQueryResponse = new URL(Encoder.encodeUrl(identifierQueryUrl)).getText("UTF-8")
        def identifierJson = js.parseText(identifierQueryResponse)
        def identifiers = identifierJson.response.docs


        // retrieve any variants
        def variantQueryUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&q=" +
                "taxonGuid:\"" + taxon.guid + "\"" + "&fq=idxtype:" + IndexDocType.TAXONVARIANT.name()
        def variantQueryResponse = new URL(Encoder.encodeUrl(variantQueryUrl)).getText("UTF-8")
        def variantJson = js.parseText(variantQueryResponse)
        def variants = variantJson.response.docs
        
        //Dataset index
        def datasetMap = [:]
        def taxonDatasetURL = getDataset(taxon.datasetID, datasetMap)?.guid
        def taxonDatasetName = getDataset(taxon.datasetID, datasetMap)?.name

        // Conservation status map
        def clists = conservationListsSource.lists ?: []
        def clists_fieldVal = clists.findAll{ it.sourceField != '*' } /* exclude simple list-membership entries */

        def conservationStatus = clists_fieldVal.inject([:], { ac, cl ->
            final cs = taxon[cl.field]
            if (cs)
                ac.put(cl.label, [dr: cl.uid, status: cs])
            ac
        })

        def model = [
                taxonConcept:[
                        guid: taxon.guid,
                        parentGuid: taxon.parentGuid,
                        nameString: taxon.scientificName,
                        nameComplete: taxon.nameComplete,
                        nameFormatted: taxon.nameFormatted,
                        author: taxon.scientificNameAuthorship,
                        nomenclaturalCode: taxon.nomenclaturalCode,
                        taxonomicStatus: taxon.taxonomicStatus,
                        nomenclaturalStatus: taxon.nomenclaturalStatus,
                        rankString: taxon.rank,
                        nameAuthority: taxon.datasetName ?: taxonDatasetName ?: grailsApplication.config.defaultNameSourceAttribution,
                        rankID:taxon.rankID,
                        nameAccordingTo: taxon.nameAccordingTo,
                        nameAccordingToID: taxon.nameAccordingToID,
                        namePublishedIn: taxon.namePublishedIn,
                        namePublishedInYear: taxon.namePublishedInYear,
                        namePublishedInID: taxon.namePublishedInID,
                        taxonRemarks: taxon.taxonRemarks,
                        provenance: taxon.provenance,
                        infoSourceURL: taxon.source ?: taxonDatasetURL,
                        datasetURL: taxonDatasetURL
                ],
                taxonName:[],
                classification: classification,
                synonyms:synonyms.collect { synonym ->
                    def datasetURL = getDataset(synonym.datasetID, datasetMap)?.guid
                    def datasetName = getDataset(synonym.datasetID, datasetMap)?.name
                    [
                            nameString: synonym.scientificName,
                            nameComplete: synonym.nameComplete,
                            nameFormatted: synonym.nameFormatted,
                            nameGuid: synonym.guid,
                            nomenclaturalCode: synonym.nomenclaturalCode,
                            taxonomicStatus: synonym.taxonomicStatus,
                            nomenclaturalStatus: synonym.nomenclaturalStatus,
                            nameAccordingTo: synonym.nameAccordingTo,
                            nameAccordingToID: synonym.nameAccordingToID,
                            namePublishedIn: synonym.namePublishedIn,
                            namePublishedInYear: synonym.namePublishedInYear,
                            namePublishedInID: synonym.namePublishedInID,
                            nameAuthority: synonym.datasetName ?: datasetName ?: grailsApplication.config.synonymSourceAttribution,
                            taxonRemarks: synonym.taxonRemarks,
                            provenance: synonym.provenance,
                            infoSourceURL: synonym.source ?: datasetURL,
                            datasetURL: datasetURL
                    ]
                },
                commonNames: commonNames.collect { commonName ->
                    def datasetURL = getDataset(commonName.datasetID, datasetMap)?.guid
                    def datasetName = getDataset(commonName.datasetID, datasetMap)?.name
                    [
                            nameString: commonName.name,
                            status: commonName.status,
                            priority: commonName.priority,
                            language: commonName.language ?: grailsApplication.config.commonNameDefaultLanguage,
                            temporal: commonName.temporal,
                            locationID: commonName.locationID,
                            locality: commonName.locality,
                            countryCode: commonName.countryCode,
                            sex: commonName.sex,
                            lifeStage: commonName.lifeStage,
                            isPlural: commonName.isPlural,
                            organismPart: commonName.organismPart,
                            taxonRemarks: commonName.taxonRemarks,
                            provenance: commonName.provenance,
                            labels: commonName.labels,
                            infoSourceName: commonName.datasetName ?: datasetName ?: grailsApplication.config.commonNameSourceAttribution,
                            infoSourceURL: commonName.source ?: datasetURL,
                            datasetURL: datasetURL
                    ]
                },
                imageIdentifier: taxon.image,
                conservationStatuses:conservationStatus,
                extantStatuses: [],
                habitats: [],
                categories: [],
                simpleProperties: [],
                images: [],
                identifiers: identifiers.collect { identifier ->
                    def datasetURL = getDataset(identifier.datasetID, datasetMap)?.guid
                    def datasetName = getDataset(identifier.datasetID, datasetMap)?.name
                    [
                            identifier: identifier.guid,
                            nameString: identifier.name,
                            status: identifier.status,
                            subject: identifier.subject,
                            format: identifier.format,
                            provenance: identifier.provenance,
                            infoSourceName: identifier.datasetName ?: datasetName ?: grailsApplication.config.identifierSourceAttribution,
                            infoSourceURL: identifier.source ?: datasetURL,
                            datasetURL: datasetURL
                    ]
                },
                variants: variants.collect { variant ->
                    def datasetURL = getDataset(variant.datasetID, datasetMap)?.guid
                    def datasetName = getDataset(variant.datasetID, datasetMap)?.name
                    [
                            nameString: variant.scientificName,
                            nameComplete: variant.nameComplete,
                            nameFormatted: variant.nameFormatted,
                            identifier: variant.guid,
                            nomenclaturalCode: variant.nomenclaturalCode,
                            taxonomicStatus: variant.taxonomicStatus,
                            nomenclaturalStatus: variant.nomenclaturalStatus,
                            nameAccordingTo: variant.nameAccordingTo,
                            nameAccordingToID: variant.nameAccordingToID,
                            namePublishedIn: variant.namePublishedIn,
                            namePublishedInYear: variant.namePublishedInYear,
                            namePublishedInID: variant.namePublishedInID,
                            nameAuthority: variant.datasetName ?: datasetName ?: grailsApplication.config.variantSourceAttribution,
                            taxonRemarks: variant.taxonRemarks,
                            provenance: variant.provenance,
                            infoSourceName: variant.datasetName ?: datasetName ?: grailsApplication.config.variantSourceAttribution,
                            infoSourceURL: variant.source ?: datasetURL,
                            datasetURL: datasetURL,
                            priority: variant.priority
                    ]
                }

        ]
        if (taxon.taxonConceptID)
            model.taxonConcept["taxonConceptID"] = taxon.taxonConceptID
        if (taxon.scientificNameID)
            model.taxonConcept["scientificNameID"] = taxon.scientificNameID
        if (taxon.acceptedConceptID)
            model.taxonConcept["acceptedConceptID"] = taxon.acceptedConceptID
        if (taxon.acceptedConceptName)
            model.taxonConcept["acceptedConceptName"] = taxon.acceptedConceptName

        if(getAdditionalResultFields()) {
            def doc = [:]
            getAdditionalResultFields().each { field ->
                if (taxon."${field}") {
                    doc.put(field, taxon."${field}")
                }
            }

            model << doc
        }

        model
    }

    /**
     * Retrieve a classification for the supplied taxonID.
     *
     * @param taxonID
     */
    def getClassification(taxonID){
        def classification = []
        def taxon = retrieveTaxon(taxonID)

        if (!taxon) return classification // empty list

        classification.add(0, [
                rank : taxon.rank,
                rankID : taxon.rankID,
                scientificName : taxon.scientificName,
                guid:taxonID
        ])

        //get parents
        def parentGuid = taxon.parentGuid
        def stop = false

        while(parentGuid && !stop){
            taxon = retrieveTaxon(parentGuid)
            if(taxon) {
                classification.add(0, [
                        rank : taxon.rank,
                        rankID : taxon.rankID,
                        scientificName : taxon.scientificName,
                        guid : taxon.guid
                ])
                parentGuid = taxon.parentGuid
            } else {
                stop = true
            }
        }
        classification
    }

    private def formatFacets(Map facetFields, List requestedFacets = []){
        def formatted = []

        if (requestedFacets) {
            // maintain order of facets from facets request parameter
            requestedFacets.each { facetName ->
                if (facetFields.containsKey(facetName)) {
                    def arrayValues = facetFields.get(facetName)
                    def facetValues = []
                    for (int i = 0; i < arrayValues.size(); i += 2) {
                        facetValues << [label: arrayValues[i], count: arrayValues[i + 1], fieldValue: arrayValues[i]]
                    }
                    formatted << [
                            fieldName  : facetName,
                            fieldResult: facetValues
                    ]
                }
            }

        }

        // Catch any remaining facets OR if requestedFacets is empty (not specified)
        facetFields.each { facetName, arrayValues ->
            if (!requestedFacets.contains(facetName)) {
                def facetValues = []
                for (int i =0; i < arrayValues.size(); i+=2){
                    facetValues << [label:arrayValues[i], count: arrayValues[i+1], fieldValue:arrayValues[i] ]
                }
                formatted << [
                        fieldName: facetName,
                        fieldResult: facetValues
                ]
            }
        }

        formatted
    }

    /**
     * Munge SOLR document set for output via JSON
     *
     * @param docs
     * @param highlighting
     * @return
     */
    private List formatDocs(docs, highlighting, params) {

        def formatted = []
        def fields = params?.fields?.split(",")?.collect({ String f -> f.trim() }) as Set

        // add occurrence counts
        if(grailsApplication.config.occurrenceCounts.enabled.asBoolean()){
            docs = populateOccurrenceCounts(docs, params)
        }

        docs.each {
            Map doc = null
            if (it.idxtype == IndexDocType.TAXON.name()) {

                def commonNameSingle = ""
                def commonNames = ""
                if (it.commonNameSingle)
                    commonNameSingle = it.commonNameSingle
                if (it.commonName) {
                    commonNames = it.commonName.join(", ")
                    if (commonNameSingle.isEmpty())
                        commonNameSingle = it.commonName.first()
                }

                doc = [
                        "id"                      : it.id, // needed for highlighting
                        "guid"                    : it.guid,
                        "linkIdentifier"          : it.linkIdentifier,
                        "idxtype"                 : it.idxtype,
                        "name"                    : it.scientificName,
                        "kingdom"                 : it.rk_kingdom,
                        "nomenclaturalCode"       : it.nomenclaturalCode,
                        "scientificName"          : it.scientificName,
                        "scientificNameAuthorship": it.scientificNameAuthorship,
                        "author"                  : it.scientificNameAuthorship,
                        "nameComplete"            : it.nameComplete,
                        "nameFormatted"           : it.nameFormatted,
                        "taxonomicStatus"         : it.taxonomicStatus,
                        "nomenclaturalStatus"     : it.nomenclaturalStatus,
                        "parentGuid"              : it.parentGuid,
                        "rank"                    : it.rank,
                        "rankID"                  : it.rankID ?: -1,
                        "commonName"              : commonNames,
                        "commonNameSingle"        : commonNameSingle,
                        "occurrenceCount"         : it.occurrenceCount,
                        "conservationStatus"      : it.conservationStatus,
                        "infoSourceName"          : it.datasetName,
                        "infoSourceURL"           : "${grailsApplication.config.collectoryBaseUrl}/public/show/${it.datasetID}"
                ]

                if (it.acceptedConceptID) {
                    doc.put("acceptedConceptID", it.acceptedConceptID)
                    if (it.acceptedConceptName)
                        doc.put("acceptedConceptName", it.acceptedConceptName)
                    doc.put("guid", it.acceptedConceptID)
                    doc.put("linkIdentifier", null)  // Otherwise points to the synonym
                }

                if (it.image) {
                    doc.put("image", it.image)
                    doc.put("imageUrl", "${grailsApplication.config.imageSmallUrl}${it.image}")
                    doc.put("thumbnailUrl", "${grailsApplication.config.imageThumbnailUrl}${it.image}")
                    doc.put("smallImageUrl", "${grailsApplication.config.imageSmallUrl}${it.image}")
                    doc.put("largeImageUrl", "${grailsApplication.config.imageLargeUrl}${it.image}")
                }

                if (getAdditionalResultFields()) {
                    getAdditionalResultFields().each { field ->
                        doc.put(field, it."${field}")
                    }
                }

                //add de-normalised fields
                def map = extractClassification(it)

                doc.putAll(map)
            } else if (it.idxtype == IndexDocType.TAXONVARIANT.name()){
                doc = [
                        "id" : it.id, // needed for highlighting
                        "guid" : it.guid,
                        "taxonGuid" : it.taxonGuid,
                        "linkIdentifier" : it.linkIdentifier,
                        "idxtype": it.idxtype,
                        "name" : it.scientificName,
                        "nomenclaturalCode" : it.nomenclaturalCode,
                        "scientificName" : it.scientificName,
                        "scientificNameAuthorship" : it.scientificNameAuthorship,
                        "author" : it.scientificNameAuthorship,
                        "nameComplete" : it.nameComplete,
                        "nameFormatted" : it.nameFormatted,
                        "taxonomicStatus" : it.taxonomicStatus,
                        "nomenclaturalStatus" : it.nomenclaturalStatus,
                        "rank": it.rank,
                        "rankID": it.rankID ?: -1,
                        "infoSourceName" : it.datasetName,
                        "infoSourceURL" : "${grailsApplication.config.collectoryBaseUrl}/public/show/${it.datasetID}"
                ]
            } else {
                doc = [
                        id            : it.id,
                        guid          : it.guid,
                        linkIdentifier: it.linkIdentifier,
                        idxtype       : it.idxtype,
                        name          : it.name,
                        description   : it.description
                ]
                if (it.taxonGuid) {
                    doc.put("taxonGuid", it.taxonGuid)
                }
                if (it.centroid) {
                    doc.put("centroid", it.centroid)
                }

                if (getAdditionalResultFields()) {
                    getAdditionalResultFields().each { field ->
                        if (it."${field}") {
                            doc.put(field, it."${field}")
                        }
                    }
                }
            }
            if (doc) {
                if (fields)
                    doc = doc.subMap(fields)
                formatted << doc
            }
        }

        // highlighting should be a LinkedHashMap with key being the 'id' of the matching result
        highlighting.each { k, v ->
            if (v) {
                Map found = formatted.find { it.id == k }
                if (found) {
                    List snips = []
                    v.each { field, snippetList ->
                        snips.addAll(snippetList)
                    }
                    found.put("highlight", snips.toSet().join("<br>"))
                }
            }
        }
        formatted
    }

    private def retrieveTaxon(taxonID){
        def encID = taxonID // URLEncoder.encode(taxonID, "UTF-8")
        def solrServerUrl = grailsApplication.config.indexLiveBaseUrl + "/select?wt=json&q=guid:\"" + encID + "\"+OR+linkIdentifier:\"" + encID +"\"&fq=idxtype:" + IndexDocType.TAXON.name()
        def queryResponse = new URL(Encoder.encodeUrl(solrServerUrl)).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json.response.docs[0]
    }

    private def extractClassification(queryResult) {
        def map = [:]
        def rankKey = "rank"
        log.debug "queryResult = ${queryResult.getClass().name}"
        Map thisTaxonFields = [
                scientificName: "scientificName",
                guid: "guid",
                taxonConcept: "taxonConceptID"
        ]
        if(queryResult){
            queryResult.keySet().each { key ->
                if (key.startsWith("rk_")) {
                    map.put(key.substring(3), queryResult.get(key))
                } else if (key.startsWith("rkid_")) {
                    map.put(key.substring(5) + "Guid", queryResult.get(key))
                }
            }
            thisTaxonFields.each { key, value ->
                if (queryResult.containsKey(key))
                    map.put(value, queryResult.get(key))
            }
            if (queryResult.containsKey(rankKey)) {
                map.put(queryResult.get(rankKey), queryResult.get("scientificName")) // current name in classification
                map.put(queryResult.get(rankKey) + "Guid", queryResult.get("guid"))
            }
        }
        map
    }

    def doPostWithParams(String url, Map params) {
        def conn = null
        def charEncoding = 'utf-8'
        try {
            String query = ""
            boolean first = true
            for (String name : params.keySet()) {
                query += first ? "?" : "&"
                first = false
                query += name.encodeAsURL() + "=" + params.get(name).encodeAsURL()
            }
            log.debug("doPostWithParams url = "+Encoder.encodeUrl(url + query))
            conn = new URL(url + query).openConnection()
            conn.setRequestMethod("POST")
            conn.setDoOutput(true)
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream(), charEncoding)

            wr.flush()
            def resp = conn.inputStream.text
            wr.close()
            return [resp: JSON.parse(resp?:"{}")] // fail over to empty json object if empty response string otherwise JSON.parse fails
        } catch (SocketTimeoutException e) {
            def error = [error: "Timed out calling web service. URL= ${url}."]
            log.error(error, e)
            return error
        } catch (Exception e) {
            def error = [error: "Failed calling web service. ${e.getMessage()} URL= ${url}.",
                         statusCode: conn?.responseCode?:"",
                         detail: conn?.errorStream?.text]
            log.error(error, e)
            return error
        }
    }

    def doPostWithParamsExc(String url, Map params) throws Exception {
        def conn = null
        def charEncoding = 'utf-8'
        String query = ""
        boolean first = true
        for (String name : params.keySet()) {
            query += first ? "?" : "&"
            first = false
            query += name.encodeAsURL() + "=" + params.get(name).encodeAsURL()
        }
        log.debug(url + query)
        conn = new URL(url + query).openConnection()
        conn.setRequestMethod("POST")
        conn.setDoOutput(true)
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

        OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream(), charEncoding)

        wr.flush()
        def resp = conn.inputStream.text
        wr.close()
        return [resp: JSON.parse(resp?:"{}")] // fail over to empty json object if empty response string otherwise JSON.parse fails
    }

    def getDataset(String datasetID, Map datasets, boolean offline = false) {
        if (!datasetID)
            return null
        def dataset = datasets.get(datasetID)
        if (!dataset) {
            def datasetQueryUrl = (offline ? grailsApplication.config.indexOfflineBaseUrl : grailsApplication.config.indexLiveBaseUrl) + "/select?wt=json&q=" +
                    "datasetID:\"" + datasetID + "\"" + "&fq=idxtype:" + IndexDocType.DATARESOURCE.name()
            def datasetQueryResponse = new URL(Encoder.encodeUrl(datasetQueryUrl)).getText("UTF-8")
            def js = new JsonSlurper()
            def datasetJson = js.parseText(datasetQueryResponse)
            dataset = datasetJson.response.docs && datasetJson.response.docs.size() > 0 ? datasetJson.response?.docs?.get(0) : null
            datasets.put(datasetID, dataset)
        }
        return dataset
    }

    /**
     * Add occurrence counts for taxa in results list
     * Taken from https://github.com/AtlasOfLivingAustralia/bie-service/blob/master/src/main/java/org/ala/web/SearchController.java#L369
     *
     * @param docs
     */
    private populateOccurrenceCounts(List docs, requestParams) {
        List guids = []
        docs.each {
            if (it.idxtype == IndexDocType.TAXON.name() && it.guid) {
                guids.add(it.guid)
            }
        }

        if (guids.size() > 0) {
            try {
                def url = "${grailsApplication.config.biocacheService.baseUrl}/occurrences/taxaCount"
                Map params = [:]
                params.put("guids", guids.join(","))
                params.put("separator", ",")

                //check for a biocache query context
                if (requestParams?.bqc){
                    params.put("fq", requestParams.bqc)
                }

                Map results = doPostWithParams(url, params) // returns (JsonObject) Map with guid as key and count as value
                Map guidsCountsMap = results.get("resp")?:[:]
                docs.each {
                    if (it.idxtype == IndexDocType.TAXON.name() && it.guid && guidsCountsMap.containsKey(it.guid))
                        it.put("occurrenceCount", guidsCountsMap.get(it.guid))
                }
            } catch (Exception ex) {
                // do nothing but log it
                log.error("Error populating occurrence counts: ${ex.message}", ex);
            }
        }
        docs
    }

    /**
     * Perform a cursor based SOLR search. USe of cursor results in more efficient and faster deep pagination of search results.
     * Which is useful for iterating over a large search results set.
     *
     * @param query
     * @param filterQuery
     * @param rows
     * @param cursor
     * @param useOfflineIndex
     */
    def getCursorSearchResults(MapSolrParams params, Boolean useOfflineIndex = false) throws Exception {
        def indexServerUrlPrefix = grailsApplication.config.indexLiveBaseUrl
        if (useOfflineIndex) {
            indexServerUrlPrefix = grailsApplication.config.indexOfflineBaseUrl
        }
        def solrServerUrl = indexServerUrlPrefix + "/select" + params.toQueryString() // toQueryString() performs encoding
        log.debug "SOLR url = ${solrServerUrl}"
        def queryResponse = new URL(solrServerUrl).getText("UTF-8")
        def js = new JsonSlurper()
        def json = js.parseText(queryResponse)
        json
    }

    def getAdditionalResultFields(){
        if(additionalResultFields == null){
            //initialise
            def fields = grailsApplication.config.additionalResultFields.split(",").findAll { !it.isEmpty() }
            additionalResultFields = fields.collect { it }
        }
        additionalResultFields
    }
}
