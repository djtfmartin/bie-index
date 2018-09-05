package au.org.ala.bie.util

import groovy.json.JsonSlurper
import org.slf4j.LoggerFactory

/**
 * A source of conservation status information.
 *
 * @author Doug Palmer &lt;Doug.Palmer@csiro.au&gt;
 *
 * @copyright Copyright (c) 2016 CSIRO
 */
class ConservationListsSource {
    static log = LoggerFactory.getLogger(ConservationListsSource.class)

    def defaultSourceField = 'status'
    def deleteFirst = ''
    def lists = []

    /**
     * Create from a JSON URL
     *
     * @param url The URL. If without a
     */
    ConservationListsSource(String url) {
        try {
            URL source = this.class.getResource(url)
            if (!source)
                source = new URL(url)
            log.info("Loading conservation lists from ${url} -> ${source}")
            JsonSlurper slurper = new JsonSlurper()
            def config = slurper.parse(source)
            defaultSourceField = config?.defaultSourceField ?: 'status'
            deleteFirst = config?.deleteFirst ?: ''
            lists = config?.lists ?: []
            log.info("Loaded " + lists.size() + " lists")
        } catch (Exception ex) {
            log.error("Unable to inifialise conservation status source from " + url, ex)
        }

    }
}
