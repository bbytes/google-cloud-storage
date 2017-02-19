package com.bbytes.google.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Simple wrapper around the Google Cloud Storage API
 */
public class GoogleCloudStorage {

	private static Properties properties;
	private static Storage storage;

	private static final String PROJECT_ID = "project.id";
	private static final String CREDENTIAL_JSON_PATH = "credential.json.path";
	
	public static final String FILE_SEPARATOR = "/";

	public Blob addFile(String bucketName, String foldername, String fileName, String filePath) throws Exception {
		return addFile(bucketName, foldername + FILE_SEPARATOR + fileName, filePath);
	}

	public Blob addFile(String bucketName, String foldername, String fileName, InputStream fileStream) throws Exception {
		return addFile(bucketName, foldername +FILE_SEPARATOR + fileName, fileStream);
	}

	public Blob addFile(String bucketName, String foldername, String fileName, InputStream fileStream, String contentType)
			throws Exception {
		return addFile(bucketName, foldername +FILE_SEPARATOR + fileName, fileStream, contentType);
	}

	public Blob addFile(String bucketName, String foldername, String fileName, byte[] data) throws Exception {
		return addFile(bucketName, foldername +FILE_SEPARATOR + fileName, data);

	}

	public Blob addFile(String bucketName, String foldername, String fileName, byte[] data, String contentType) throws Exception {
		return addFile(bucketName, foldername +FILE_SEPARATOR + fileName, data, contentType);
	}

	public Blob addFile(String bucketName, String blobName, String filePath) throws Exception {
		File file = new File(filePath);
		InputStream stream = new FileInputStream(file);
		return addFile(bucketName, blobName, stream);
	}

	public Blob addFile(String bucketName, String blobName, InputStream fileStream) throws Exception {
		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.create(blobName, fileStream);
	}

	public Blob addFile(String bucketName, String fileName, InputStream fileStream, String contentType) throws Exception {
		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.create(fileName, fileStream, contentType);
	}

	public Blob addFile(String bucketName, String fileName, byte[] data) throws Exception {

		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.create(fileName, data);

	}

	public Blob addFile(String bucketName, String fileName, byte[] data, String contentType) throws Exception {
		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.create(fileName, data, contentType);
	}

	public Blob createFolder(String bucketName, String foldername) throws Exception {
		BlobId blobId = BlobId.of(bucketName, foldername +FILE_SEPARATOR);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		return getStorage().create(blobInfo);
	}

	public Blob getFile(String bucketName, String fileName) throws Exception {

		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.get(fileName);
	}

	public Blob getFile(String bucketName, String folderName, String fileName) throws Exception {

		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.get(folderName +FILE_SEPARATOR + fileName);
	}

	public File getAsLocalFile(String bucketName, String fileName, String destinationDirectory) throws Exception {

		File directory = new File(destinationDirectory);
		if (!directory.isDirectory()) {
			throw new Exception("Provided destinationDirectory path is not a directory");
		}
		File file = new File(directory.getAbsolutePath() +FILE_SEPARATOR + fileName);

		Blob blob = getFile(bucketName, fileName);
		if (blob == null)
			return null;

		boolean append = false;
		FileOutputStream fos = new FileOutputStream(file, append);
		FileChannel channel = fos.getChannel();

		ReadChannel reader = blob.reader();

		try {
			ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
			while (reader.read(bytes) > 0) {
				bytes.flip();
				channel.write(bytes);
				bytes.clear();
			}
		} finally {
			channel.close();
			fos.close();
		}

		return file;

	}

	/**
	 * Deletes a file within a bucket
	 * 
	 * @param bucketName
	 *            Name of bucket that contains the file
	 * @param fileName
	 *            The file to delete
	 * @throws Exception
	 */
	public boolean deleteFile(String bucketName, String fileName) throws Exception {
		Blob blob = getFile(bucketName, fileName);
		if (blob == null || !blob.exists())
			return false;

		return blob.delete();

	}

	/**
	 * Creates a bucket
	 * 
	 * @param bucketName
	 *            Name of bucket to create
	 * @throws Exception
	 */
	public Bucket createBucket(String bucketName) throws Exception {
		Storage storage = getStorage();
		Bucket bucket = getBucket(bucketName);
		if (bucket != null && bucket.exists()) {
			return bucket;
		}
		return storage.create(BucketInfo.of(bucketName));
	}

	public Bucket getBucket(String bucketName) throws Exception {
		Storage storage = getStorage();
		return storage.get(bucketName);
	}

	/**
	 * Deletes a bucket
	 * 
	 * @param bucketName
	 *            Name of bucket to delete
	 * @throws Exception
	 */
	public boolean deleteBucket(String bucketName) throws Exception {
		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return false;

		return bucket.delete();

	}

	/**
	 * Example of checking if the bucket exists.
	 * 
	 * @throws Exception
	 */
	public boolean bucketExists(String bucketName) throws Exception {
		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return false;

		return bucket.exists();
	}

	public List<Blob> getFiles(String bucketName, Iterable<String> fileNames) throws Exception {

		Bucket bucket = getBucket(bucketName);
		if (bucket == null)
			return null;

		return bucket.get(fileNames);
	}

	public Page<Blob> listFiles(String bucketName) throws Exception {
		Storage storage = getStorage();
		Bucket bucket = storage.get(bucketName);
		return bucket.list();
	}

	/**
	 * List the buckets with the project (Project is configured in properties)
	 * 
	 * @return
	 * @throws Exception
	 */
	public Page<Bucket> listBuckets() throws Exception {
		Storage storage = getStorage();
		return storage.list();
	}

	private static Properties getProperties() throws Exception {

		if (properties == null) {
			properties = new Properties();
			InputStream stream = GoogleCloudStorage.class.getResourceAsStream("/cloudstorage.properties");
			try {
				properties.load(stream);
			} catch (IOException e) {
				throw new RuntimeException("cloudstorage.properties must be present in classpath", e);
			} finally {
				stream.close();
			}
		}
		return properties;
	}

	private Storage getStorage() throws Exception {

		if (storage == null) {
			GoogleCredentials credential = getCredential();
			storage = StorageOptions.newBuilder().setProjectId(getProperties().getProperty(PROJECT_ID)).setCredentials(credential).build()
					.getService();
		}
		return storage;
	}

	private GoogleCredentials getCredential() throws Exception {

		ServiceAccountCredentials credentials = ServiceAccountCredentials
				.fromStream(GoogleCloudStorage.class.getResourceAsStream(getProperties().getProperty(CREDENTIAL_JSON_PATH)));
		credentials.createScoped(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
		return credentials;

	}
}
