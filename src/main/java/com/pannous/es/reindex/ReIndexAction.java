package com.pannous.es.reindex;

import org.json.JSONException;
import org.json.JSONObject;
import java.io.UnsupportedEncodingException;
import java.util.*;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.*;
import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;

/**
 * Refeeds all the documents which matches the type and the (optional) query.
 *
 * @author Peter Karich
 */
public class ReIndexAction extends BaseRestHandler {

    @Inject public ReIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        if (controller != null) {
            // Define REST endpoints to do a reindex
            controller.registerHandler(PUT, "/{index}/{type}/_reindex", this);
            controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
        }
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel, Client client) {
        handleRequest(request, channel, null, false, client, null);
    }

    private int hitsPerPage = 0;

    private String[] emailSendSkips = {"profileData", "viaMessageBus"};

    private List<String> emailSendSkipList = Arrays.asList(emailSendSkips);

    private String[] userSkips = {
            "ab_group",
            "ab_seg",
            "AprilBlankEmailGroup",
            "collectionProductRecos",
            "coupon",
            "credit3_amount",
            "credit_amount",
            "dataFields",
            "dbi_late2_order_number",
            "dbi_late2_sku1_image_url",
            "dbi_late2_sku1_name",
            "dbi_late2_sku1_qty",
            "dbi_late2_sku1_sku",
            "dbi_late2_sku1_url",
            "dbi_late2_sku2_image_url",
            "dbi_late2_sku2_name",
            "dbi_late2_sku2_qty",
            "dbi_late2_sku2_sku",
            "dbi_late2_sku2_url",
            "dbi_late2_sku3_image_url",
            "dbi_late2_sku3_name",
            "dbi_late2_sku3_qty",
            "dbi_late2_sku3_sku",
            "dbi_late2_sku3_url",
            "dbi_late2_sku4_image_url",
            "dbi_late2_sku4_name",
            "dbi_late2_sku4_qty",
            "dbi_late2_sku4_sku",
            "dbi_late2_sku4_url",
            "dbi_late3_order_number",
            "dbi_late3_sku1_image_url",
            "dbi_late3_sku1_name",
            "dbi_late3_sku1_qty",
            "dbi_late3_sku1_sku",
            "dbi_late3_sku1_url",
            "dbi_late_new_est_del_date_sku1",
            "dbi_late_new_est_del_date_sku2",
            "dbi_late_new_est_del_date_sku3",
            "dbi_late_new_est_del_date_sku4",
            "dbi_late_order_delaytime",
            "dbi_late_order_number",
            "dbi_late_sku1_image_url",
            "dbi_late_sku1_name",
            "dbi_late_sku1_qty",
            "dbi_late_sku1_sku",
            "dbi_late_sku1_url",
            "dbi_late_sku2_image_url",
            "dbi_late_sku2_name",
            "dbi_late_sku2_qty",
            "dbi_late_sku2_sku",
            "dbi_late_sku2_url",
            "dbi_late_sku3_image_url",
            "dbi_late_sku3_name",
            "dbi_late_sku3_qty",
            "dbi_late_sku3_sku",
            "dbi_late_sku3_url",
            "dbi_late_sku4_image_url",
            "dbi_late_sku4_name",
            "dbi_late_sku4_qty",
            "dbi_late_sku4_sku",
            "dbi_late_sku4_url",
            "campaignId",
            "templateId",
            "Engagement",
            "exception",
            "facebook_code",
            "fields",
            "foo",
            "lastActiveAt",
            "lastBrowsedProducts",
            "messageBusId",
            "Name",
            "personalizedDrip",
            "recipientState",
            "recovery_credit",
            "recovery_OrderNum",
            "region",
            "sailthru_engagement_level",
            "sailthruLastClick",
            "sailthruLastOpen",
            "transactionalData",
            "twitter_code",
            "userProductRecos",
            "value",
            "welcomeDripDay",
            "wf408Drip1",
            "wf408Drip2",
            "wf408Drip3",
            "wf408Drip4",
            "wf408Drip5",
            "wf78"
    };

    private List<String> userSkipList = Arrays.asList(userSkips);

    private List<String> skipFieldList = new ArrayList<String>();

    public void handleRequest(RestRequest request, RestChannel channel, String newTypeOverride, boolean internalCall, Client client, List<String> skipFieldList) {
        this.skipFieldList = skipFieldList;

        logger.info("Skip field list", skipFieldList);
        logger.info("ReIndexAction.handleRequest [{}]", request.params());
        String newIndexName = request.param("index");
        String searchIndexName = request.param("searchIndex");
        if (searchIndexName == null || searchIndexName.isEmpty())
            searchIndexName = newIndexName;

        String newType = newTypeOverride != null ? newTypeOverride : request.param("type");
        String searchType = newTypeOverride != null ? newTypeOverride : request.param("searchType");
        if (searchType == null || searchType.isEmpty())
            searchType = newType;

        int searchPort = request.paramAsInt("searchPort", 9200);
        String searchHost = request.param("searchHost", "localhost");
        boolean localAction = "localhost".equals(searchHost) && searchPort == 9200;
        boolean withVersion = request.paramAsBoolean("withVersion", false);
        int keepTimeInMinutes = request.paramAsInt("keepTimeInMinutes", 30);
        hitsPerPage = request.paramAsInt("hitsPerPage", 1000);
        float waitInSeconds = request.paramAsFloat("waitInSeconds", 0);
        String basicAuthCredentials = request.param("credentials", "");
        String filter = request.content().toUtf8();
        MySearchResponse rsp;
        if (localAction) {
            SearchRequestBuilder srb = createScrollSearch(searchIndexName, searchType, filter,
                    hitsPerPage, withVersion, keepTimeInMinutes, client);
            SearchResponse sr = srb.execute().actionGet();
            rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);
        } else {
            // TODO make it possible to restrict to a cluster
            rsp = new MySearchResponseJson(searchHost, searchPort, searchIndexName, searchType, filter,
                    basicAuthCredentials, hitsPerPage, withVersion, keepTimeInMinutes);
        }

        // TODO make async and allow control of process from external (e.g. stopping etc)
        // or just move stuff into a river?
        reindex(rsp, newIndexName, newType, withVersion, waitInSeconds, client);

        // TODO reindex again all new items => therefor we need a timestamp field to filter
        // + how to combine with existing filter?

        logger.info("Finished reindexing of index " + searchIndexName + " into " + newIndexName + ", query " + filter);

        if (!internalCall)
            channel.sendResponse(new BytesRestResponse(OK));
    }

    public SearchRequestBuilder createScrollSearch(String oldIndexName, String oldType, String filter,
            int hitsPerPage, boolean withVersion, int keepTimeInMinutes, Client client) {
        SearchRequestBuilder srb = client.prepareSearch(oldIndexName).
                setTypes(oldType).
                setVersion(withVersion).
                setSize(hitsPerPage).
                setSearchType(SearchType.SCAN).
                addField("_source").
                addField("_parent").
                setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes));

        if (filter != null && !filter.trim().isEmpty())
            srb.setPostFilter(filter);
        return srb;
    }

    public int reindex(MySearchResponse rsp, String newIndex, String newType, boolean withVersion,
            float waitSeconds, Client client) {
        boolean flushEnabled = false;
        long total = rsp.hits().totalHits();
        int collectedResults = 0;
        int failed = 0;
        while (true) {
            if (collectedResults > 0 && waitSeconds > 0) {
                try {
                    Thread.sleep(Math.round(waitSeconds * 1000));
                } catch (InterruptedException ex) {
                    break;
                }
            }
            StopWatch queryWatch = new StopWatch().start();
            int currentResults = rsp.doScoll();
            if (currentResults == 0)
                break;

            MySearchHits res = callback(rsp.hits(), newType);
            if (res == null)
                break;
            queryWatch.stop();
            StopWatch updateWatch = new StopWatch().start();
            failed += bulkUpdate(res, newIndex, newType, withVersion, client).size();
            if (flushEnabled)
                client.admin().indices().flush(new FlushRequest(newIndex)).actionGet();

            updateWatch.stop();
            collectedResults += currentResults;
            logger.debug("Progress " + collectedResults + "/" + total
                    + ". Time of update:" + updateWatch.totalTime().getSeconds() + " query:"
                    + queryWatch.totalTime().getSeconds() + " failed:" + failed);
        }
        String str = "found " + total + ", collected:" + collectedResults
                + ", transfered:" + (float) rsp.bytes() / (1 << 20) + "MB";
        if (failed > 0)
            logger.warn(failed + " FAILED documents! " + str);
        else
            logger.info(str);
        return collectedResults;
    }

    Collection<Integer> bulkUpdate(MySearchHits objects, String indexName,
            String newType, boolean withVersion, Client client) {
        BulkRequestBuilder brb = client.prepareBulk();
        for (MySearchHit hit : objects.getHits()) {
            if (hit.id() == null || hit.id().isEmpty()) {
                logger.warn("Skipped object without id when bulkUpdate:" + hit);
                continue;
            }

            try {
                IndexRequest indexReq = Requests.indexRequest(indexName).type(newType).id(hit.id()).source(hit.source());
                if (withVersion)
                    indexReq.version(hit.version());
                if (hit.parent() != null && !hit.parent().isEmpty()) {
                    indexReq.parent(hit.parent());
                }
                brb.add(indexReq);
            } catch (Exception ex) {
                logger.warn("Cannot add object:" + hit + " to bulkIndexing action." + ex.getMessage());
            }
        }
        if (brb.numberOfActions() > 0) {
            BulkResponse rsp = brb.execute().actionGet();
            if (rsp.hasFailures()) {
                List<Integer> list = new ArrayList<Integer>(rsp.getItems().length);
                for (BulkItemResponse br : rsp.getItems()) {
                    if (br.isFailed())
                        list.add(br.getItemId());
                }
                return list;
            }
        }
        return Collections.emptyList();
    }

    /**
     * Can be used to be overwritten and to rewrite some fields of the hits.
     */
    protected MySearchHits callback(MySearchHits hits, String type) {
        List<String> skips;

        if (type.equals("user")) {
            skips = userSkipList;

        } else if (type.equals("emailSend")) {
            skips = emailSendSkipList;

        } else {
            skips = skipFieldList;
        }


        if (skips.size() > 0) {
            SimpleList res = new SimpleList(hitsPerPage, hits.totalHits());
            for (MySearchHit h : hits.getHits()) {
                try {
                    String str = new String(h.source(), charset);
                    RewriteSearchHit newHit = new RewriteSearchHit(h.id(), h.parent(), h.version(), str);

                    for (String ignoreField: skips) {
                        newHit.remove(ignoreField);
                    }
                    res.add(newHit);
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }

            return res;
        } else {
            return hits;
        }
    }

    public static class SimpleList implements MySearchHits {

        long totalHits;
        List<MySearchHit> hits;

        public SimpleList(int size, long total) {
            hits = new ArrayList<MySearchHit>(size);
            totalHits = total;
        }

        public void add(MySearchHit hit) {
            hits.add(hit);
        }

        @Override public Iterable<MySearchHit> getHits() {
            return hits;
        }

        @Override
        public long totalHits() {
            return totalHits;
        }
    }

    private final static String charset = "UTF-8";

    public static class RewriteSearchHit implements MySearchHit {

        String id;
        String parent;
        long version;
        JSONObject json;

        public RewriteSearchHit(String id, String parent, long version, String jsonStr) {
            this.id = id;
            this.version = version;
            this.parent = parent;
            try {
                json = new JSONObject(jsonStr);
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        public String get(String key) {
            try {
                if (!json.has(key))
                    return "";
                String val = json.getString(key);
                if (val == null)
                    return "";
                return val;
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        public void remove(String key) {
            json.remove(key);
        }

        public JSONObject put(String key, Object obj) {
            try {
                return json.put(key, obj);
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override public String id() {
            return id;
        }

        @Override public String parent() {
            return parent;
        }
        @Override public long version() {
            return version;
        }

        @Override public byte[] source() {
            try {
                return json.toString().getBytes(charset);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
