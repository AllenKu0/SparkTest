package org.example.http;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.example.utils.Utils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * {@link SparkRefineHTTPClient} is a wrapper to the RefineOnSpark client,
 * modified to conform with the Refine Driver Program {@link RefineOnSpark}
 * 
 */
public class SparkRefineHTTPClient {

	private static final Logger fLogger = LoggerFactory
			.getLogger(SparkRefineHTTPClient.class.getSimpleName());

	/**
	 * Poll intervals for asynchronous operations should start at
	 * {@link #MIN_POLL_INTERVAL}, and grow exponentially at every iteration
	 * until they reach {@link #MAX_POLL_INTERVAL}.
	 */
	private static final int MIN_POLL_INTERVAL = 500;

	/**
	 * Maximum poll interval length for asynchronous operations.
	 */
	private static final int MAX_POLL_INTERVAL = 3000;

	/**
	 * Length of the random identifier used for temporary refine projects.
	 */
	private static final int IDENTIFIER_LENGTH = 10;

	private final CloseableHttpClient fHttpClient;

	private final URI fRefineURI;

	/**
	 * Creates a new {@link RefineHTTPClient}.
	 * 
	 * @param host
	 *            host where the remote engine is running.
	 * 
	 * @param port
	 *            port at which the remote engine is running.
	 * 
	 * @throws URISyntaxException
	 *             if the host name contains illegal syntax.
	 */
	public SparkRefineHTTPClient(String host, int port)
			throws URISyntaxException {
		fRefineURI = new URI("http", null, host, port, null, null, null);
		fHttpClient = HttpClients.createDefault();
	}

	public List<String> transform(ContentBody chunk, JSONArray transform,
			Properties exporterOptions) throws IOException, JSONException {
		String handle = null;
		List<String> transformed;
		try {
			//回UTF-8轉換過的URI,URI:response的Location的欄位，看起來是創project
			handle = createProjectAndUpload(chunk);

			//把project傳入並執行對應轉換，如果狀態code為pending的話為true
			if (applyOperations(handle, transform)) {
				//找pending的process，並停一下等事情做完
				join(handle);
			}
			//將結果回傳
			transformed = outputResults(handle, exporterOptions);
		} finally {
			//結束project
			deleteProject(handle);
		}
		return transformed;
	}

	//將ContentBody發post,URI:/command/core/create-project-from-upload
	//回UTF-8轉換過的URI,URI:response的Location的欄位
	private String createProjectAndUpload(ContentBody original)
			throws IOException {
		CloseableHttpResponse response = null;

		try {
			/*
			 * Refine requires projects to be named, but these are not important
			 * for us, so we just use a random string.
			 */
			//生成只有字母的亂數符，不過不建議使用
			String name = RandomStringUtils
					.randomAlphanumeric(IDENTIFIER_LENGTH);

			//addPart 以Key-Value 傳ContentType 把rdd傳進去形成payLoad
			HttpEntity entity = MultipartEntityBuilder
					.create()
					.addPart("project-file", original)
					.addPart("project-name",
							new StringBody(name, ContentType.TEXT_PLAIN))
					.build();

			//post (uri,body),response 為結果
			response = doPost("/command/core/create-project-from-upload",
					entity);

			//結果的內的Location網址，應該為project 的URI
			URI projectURI = new URI(response.getFirstHeader("Location")
					.getValue());

			// XXX is this always UTF-8 or do we have to look somewhere?
			//回UTF-8轉換過的URI
			return URLEncodedUtils.parse(projectURI, "UTF-8").get(0).getValue();
		} catch (Exception e) {
			throw launderedException(e);
		} finally {
			//關閉post
			Utils.safeClose(response, false);
		}
	}

	//執行任務
	private boolean applyOperations(String handle, JSONArray transform)
			throws IOException {
		//監聽http action 結果
		CloseableHttpResponse response = null;

		try {
			//Key-Value
			List<NameValuePair> pairs = new ArrayList<NameValuePair>();
			pairs.add(new BasicNameValuePair("project", handle));
			pairs.add(new BasicNameValuePair("operations", transform.toString()));

			response = doPost("/command/core/apply-operations",
					new UrlEncodedFormEntity(pairs));

			//把response 轉JSON
			JSONObject content = decode(response);
			if (content == null) {
				return false;
			}

			return "pending".equals(content.get("code"));

		} catch (Exception e) {
			throw launderedException(e);
		} finally {
			Utils.safeClose(response, false);
		}
	}

	private void join(String handle) throws IOException {
		CloseableHttpResponse response = null;

		try {
			long backoff = MIN_POLL_INTERVAL;

			while (hasPendingOperations(handle)) {
				Thread.sleep(backoff);
				backoff = Math.min(backoff * 2, MAX_POLL_INTERVAL);
			}

		} catch (Exception e) {
			throw launderedException(e);
		} finally {
			Utils.safeClose(response, false);
		}
	}

	private List<String> outputResults(String handle, Properties exporterOptions)
			throws IOException {
		CloseableHttpResponse response = null;
		List<String> transformed = null;
		try {
			//獲取key為format的值（csv）
			String format = checkedGet(exporterOptions, "format");

			List<NameValuePair> pairs = new ArrayList<NameValuePair>();

			pairs.add(new BasicNameValuePair("project", handle));
			pairs.add(new BasicNameValuePair("format", format));

			response = doPost("/command/core/export-rows/" + handle + "."
					+ format, new UrlEncodedFormEntity(pairs));

			//將結果轉成List<String>
			transformed = IOUtils.readLines(response.getEntity().getContent(),
					"UTF-8");

		} catch (Exception e) {
			throw launderedException(e);
		} finally {
			//關閉POST
			Utils.safeClose(response, false);
		}
		return transformed;

	}

	//確認獲取的key內有值
	private String checkedGet(Properties p, String key) {
		String value = p.getProperty(key);

		if (value == null) {
			throw new IllegalArgumentException("Missing required parameter "
					+ key + ".");
		}

		return value;
	}

	private void deleteProject(String handle) throws IOException {
		if (handle == null) {
			return;
		}

		CloseableHttpResponse response = null;

		try {
			List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
			urlParameters.add(new BasicNameValuePair("project", handle));
			response = doPost("/command/core/delete-project",
					new UrlEncodedFormEntity(urlParameters));

			// Not much of a point in checking the response as
			// it will contain "OK" no matter what happens.
		} catch (Exception e) {
			throw launderedException(e);
		} finally {
			Utils.safeClose(response, false);
		}
	}

	//獲取pending的操作
	private boolean hasPendingOperations(String handle) throws IOException,
			URISyntaxException, JSONException {
		CloseableHttpResponse response = null;

		try {
			HttpGet poll = new HttpGet(new URIBuilder(fRefineURI)
					.setPath("/command/core/get-processes")
					.addParameter("project", handle).build());

			logRequest(poll);

			response = logResponse(fHttpClient.execute(poll));
			JSONArray pending = decode(response).getJSONArray("processes");

			fLogger.info(formatProgress(pending));

			return pending.length() != 0;
		} finally {
			Utils.safeClose(response, true);
		}
	}

	private String formatProgress(JSONArray pending) {
		StringBuffer formatString = new StringBuffer("[Progress: ");
		Object[] progress = new Object[pending.length()];

		for (int i = 0; i < pending.length(); i++) {
			formatString.append("%1$3s%% ");
			try {
				progress[i] = ((JSONObject) pending.get(i))
						.getString("progress");
			} catch (JSONException ex) {
				progress[i] = "error";
			}
		}

		formatString.setCharAt(formatString.length() - 1, ']');

		return String.format(formatString.toString(), progress);
	}

	private CloseableHttpResponse doPost(String path, HttpEntity entity)
			throws ClientProtocolException, IOException, URISyntaxException {
		//用refine加command/core/create-project-from-upload
		URI requestURI = new URIBuilder(fRefineURI).setPath(path).build();
		//對uri發出post
		HttpPost post = new HttpPost(requestURI);

		//設定post的body
		post.setEntity(entity);
		//寫log
		logRequest(post);
		//回執行結果log
		return logResponse(fHttpClient.execute(post));
	}

	private RuntimeException launderedException(Exception ex)
			throws IOException {
		if (ex instanceof ConnectException) {
			throw (ConnectException) ex;
		}

		return new RuntimeException(ex);
	}

	//轉JSON
	private JSONObject decode(CloseableHttpResponse response)
			throws IOException {
		try {
			return new JSONObject(IOUtils.toString(response.getEntity()
					.getContent()));
		} catch (JSONException ex) {
			fLogger.error("Error decoding server response: ", ex);
			return null;
		}
	}

	//request log
	private void logRequest(HttpRequest request) {
		if (fLogger.isDebugEnabled()) {
			fLogger.debug(request.toString());
		}
	}

	//結果log
	private CloseableHttpResponse logResponse(CloseableHttpResponse response) {
		if (fLogger.isDebugEnabled()) {
			fLogger.debug(response.toString());
		}
		return response;
	}

	public void close() throws IOException {
		fHttpClient.close();
	}

}
