package com.bbytes.google.storage;

import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

import com.bbytes.google.storage.GoogleCloudStorage;
import com.google.cloud.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;

public class TestStorage {

	private GoogleCloudStorage cloudStorage = new GoogleCloudStorage();

	private final static String BUCKET_NAME = "unilog-test-bucket-12334444";

	private final static String FOLDER_NAME = "test-1";

	@Test
	public void createBucketTest() throws Exception {
		Bucket bucket = cloudStorage.createBucket(BUCKET_NAME);
		Assert.assertNotNull(bucket);
		Assert.assertTrue(bucket.getName().equals(BUCKET_NAME));
		Assert.assertTrue(cloudStorage.listBuckets().iterateAll().hasNext());
	}

	@Test
	public void createFolderTest() throws Exception {
		Blob blob = cloudStorage.createFolder(BUCKET_NAME, "test1");
		Assert.assertNotNull(blob);
		Assert.assertTrue(blob.getName().equals("test1" + GoogleCloudStorage.FILE_SEPARATOR));
	}

	@Test
	public void createFileTest() throws Exception {
		InputStream inputStream = TestStorage.class.getClassLoader().getResourceAsStream("test.txt");
		Blob blob = cloudStorage.addFile(BUCKET_NAME, FOLDER_NAME, "test-file", inputStream, "text/plain");
		Assert.assertNotNull(blob);
		Assert.assertTrue(blob.getName().equals(FOLDER_NAME + GoogleCloudStorage.FILE_SEPARATOR + "test-file"));
	}

	@Test
	public void deleteFileTest() throws Exception {
		InputStream inputStream = TestStorage.class.getClassLoader().getResourceAsStream("test.txt");
		Blob blob = cloudStorage.addFile(BUCKET_NAME, FOLDER_NAME, "test-file", inputStream, "text/plain");
		Assert.assertNotNull(blob);
		Assert.assertTrue(blob.getName().equals(FOLDER_NAME + GoogleCloudStorage.FILE_SEPARATOR + "test-file"));

		cloudStorage.deleteFile(BUCKET_NAME, blob.getName());

		Blob missingblob = cloudStorage.getFile(BUCKET_NAME, blob.getName());
		Assert.assertNull(missingblob);

	}

	@Test
	public void listFilesTest() throws Exception {
		Page<Blob> blobs = cloudStorage.listFiles(BUCKET_NAME);
		Assert.assertNotNull(blobs);
		Assert.assertTrue(blobs.iterateAll().hasNext());

		for (Blob blob : blobs.getValues()) {
			System.out.println(blob.getName());
		}
	}

}
