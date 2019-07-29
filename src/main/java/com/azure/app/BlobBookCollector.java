// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.app;

import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.data.appconfiguration.ConfigurationAsyncClient;
import com.azure.data.appconfiguration.credentials.ConfigurationClientCredentials;
import com.azure.data.appconfiguration.models.ConfigurationSetting;
import com.azure.data.appconfiguration.models.SettingSelector;
import com.azure.storage.blob.BlockBlobAsyncClient;
import com.azure.storage.blob.ContainerAsyncClient;
import com.azure.storage.blob.StorageAsyncClient;
import com.azure.storage.blob.StorageClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.credentials.SharedKeyCredential;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class BlobBookCollector implements BookCollection {
    private final Set<String> supportedImageFormats;
    private Mono<StorageAsyncClient> storageClient;
    private Mono<ContainerAsyncClient> imageContainerClient;
    private Mono<ContainerAsyncClient> bookContainerClient;
    private static Logger logger = LoggerFactory.getLogger(BlobBookCollector.class);

    BlobBookCollector() {
        supportedImageFormats = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("gif", "png", "jpg")));
        String connectionString = System.getenv("AZURE_APPCONFIG");
        if (connectionString == null || connectionString.isEmpty()) {
            System.err.println("Environment variable AZURE_APPCONFIG is not set. Cannot connect to App Configuration."
                + " Please set it.");
            return;
        }
        ConfigurationAsyncClient client;
        try {
            client = ConfigurationAsyncClient.builder()
                .credentials(new ConfigurationClientCredentials(connectionString))
                .build();
            SettingSelector keys = new SettingSelector().keys("BLOB*");
            storageClient = client.listSettings(keys).collectList().map(list -> {
                String accountName = null;
                String accountKey = null;
                String url = null;
                for (ConfigurationSetting configurationSetting : list) {
                    String key = configurationSetting.key();
                    if (key.contentEquals("BLOB_ACCOUNT_NAME")) {
                        accountName = configurationSetting.value();
                    } else if (key.contentEquals("BLOB_KEY")) {
                        accountKey = configurationSetting.value();
                    } else {
                        url = configurationSetting.value();
                    }
                }
                SharedKeyCredential credential = new SharedKeyCredential(accountName, accountKey);
                String endPoint = String.format(Locale.ROOT, url);
                return StorageClient.storageClientBuilder()
                    .endpoint(endPoint)
                    .credential(credential)
                    .httpLogDetailLevel(HttpLogDetailLevel.HEADERS)
                    .buildAsyncClient();
            });
            bookContainerClient = storageClient.flatMap(container -> {
                final ContainerAsyncClient temp = container.getContainerAsyncClient("book-library");
                return temp.exists().flatMap(exists -> {
                    if (exists.value()) {
                        return Mono.just(temp);
                    } else {
                        return temp.create().then(Mono.just(temp));
                    }
                });
            });
            imageContainerClient = storageClient.flatMap(container -> {
                final ContainerAsyncClient temp = container.getContainerAsyncClient("book-covers");
                return temp.exists().flatMap(exists -> {
                    if (exists.value()) {
                        return Mono.just(temp);
                    } else {
                        return temp.create().then(Mono.just(temp));
                    }
                });
            });
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            logger.error("Exception with App Configuration: ", e);
            return;
        }
    }

    /**
     * Returns the Flux of Book objects
     *
     * @return Flux<Book> the flux with all the book information. {@literal Flux<Book>}
     */
    @Override
    public Flux<Book> getBooks() {
        return bookContainerClient.flatMapMany(container -> container.listBlobsFlat().flatMapSequential(blob -> {
            final BlockBlobAsyncClient blockBlobClient = container.getBlockBlobAsyncClient(blob.name());
            return blockBlobClient.download().flatMapMany(byteBuff -> byteBuff.value().map(byteBuffer -> {
                Book b = Constants.SERIALIZER.fromJSONtoBook(byteBuffer);
                return b;
            }));
        }));
    }

    @Override
    public Mono<Void> saveBook(String title, Author author, URI path) {
        File relativePath = Paths.get(Constants.IMAGE_PATH, author.getLastName(), author.getFirstName(), new File(path).getName()).toFile();
        URI saved = relativePath.toURI();
        URI relative = new File(System.getProperty("user.dir")).toURI().relativize(saved);
        byte[] bookFile = Constants.SERIALIZER.writeJSON(new Book(title, author, relative));
        if (bookFile == null) {
            return Mono.error(new IllegalStateException("Couldn't convert book to file"));
        }
        String[] blobInfo = getBlobInformation(author, title + ".json");
        if (blobInfo == null) {
            return Mono.error(new IllegalArgumentException("Error encoding blob name"));
        }
        return saveImage(new File(path), author, title).then(bookContainerClient.flatMap(containerAsyncClient -> {
            final BlockBlobAsyncClient blockBlobClient = containerAsyncClient.getBlockBlobAsyncClient(blobInfo[2] + "/" + blobInfo[1]
                + "/" + blobInfo[0]);
            return blockBlobClient.upload(Flux.just(ByteBuffer.wrap(bookFile)), bookFile.length).then();
        }));
    }

    private Mono<Void> saveImage(File imagePath, Author author, String title) {
        String extension = FilenameUtils.getExtension(imagePath.getName());
        if (!supportedImageFormats.contains(extension)) {
            return Mono.error(new IllegalStateException("Error. Wrong file format for image"));
        }
        String[] blobInfo = getBlobInformation(author, title + "." + extension);
        if (blobInfo == null) {
            return Mono.error(new IllegalArgumentException("Error encoding blob name"));
        }
        return imageContainerClient.flatMap(containerAsyncClient -> {
            final BlockBlobAsyncClient blockBlobClient = containerAsyncClient.getBlockBlobAsyncClient(blobInfo[2] + "/" + blobInfo[1]
                + "/" + blobInfo[0]);
            return blockBlobClient.uploadFromFile(imagePath.getAbsolutePath()).then();
        });
    }

    private String[] getBlobInformation(Author author, String fileName) {
        String blobLastName,
            blobFirstName,
            blobName;
        try {
            blobLastName = URLEncoder.encode(author.getLastName().toLowerCase(), StandardCharsets.US_ASCII.toString());
            blobName = URLEncoder.encode(fileName, StandardCharsets.US_ASCII.toString());
            blobFirstName = URLEncoder.encode(author.getFirstName().toLowerCase(), StandardCharsets.US_ASCII.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding names: ", e);
            return null;
        }
        return new String[]{blobName, blobFirstName, blobLastName};
    }

    @Override
    public Mono<Void> editBook(Book oldBook, Book newBook, int saveCover) {
        if (saveCover == 1) {
            return deleteBook(oldBook).then(
                saveBook(newBook.getTitle(), newBook.getAuthor(), newBook.getCover()));
        } else {
            String[] blobConversion = getBlobInformation(oldBook.getAuthor(), oldBook.getTitle());
            Flux<BlobItem> file = imageContainerClient.flatMapMany(containerAsyncClient ->
                containerAsyncClient.listBlobsFlat().filter(blobItem -> blobItem.name().contains(blobConversion[2] + "/"
                    + blobConversion[1]
                    + "/" + blobConversion[0] + ".")));
            Mono<BlobItem> bookMono = file.hasElements().flatMap(notEmpty -> {
                if (notEmpty)
                    return file.elementAt(0);
                else {
                    return Mono.error(new IllegalStateException("Cover image not found."));
                }
            });
            return imageContainerClient.flatMap(containerAsyncClient ->
                bookMono.flatMap(blobItem -> {
                    final BlockBlobAsyncClient blockBlob = containerAsyncClient.getBlockBlobAsyncClient(blobItem.name());
                    String property = "java.io.tmpdir";
                    String tempDir = System.getProperty(property);
                    String newFileName;
                    try {
                        newFileName = URLEncoder.encode(newBook.getTitle(), StandardCharsets.US_ASCII.toString());
                    } catch (UnsupportedEncodingException e) {
                        logger.error("Error encoding file: ", e);
                        return Mono.error(e);
                    }
                    String extension = blobItem.name().substring(blobItem.name().lastIndexOf("."));
                    File newFile = new File(tempDir + newFileName + extension);
                    try {
                        newFile.createNewFile();
                    } catch (IOException e) {
                        logger.error("Exception creating the file: ", e);
                        return Mono.error(e);
                    }
                    return blockBlob.downloadToFile(newFile.getAbsolutePath()).then(deleteBook(oldBook)).then(saveBook(newBook.getTitle(),
                        newBook.getAuthor(), newFile.toURI()));
                }));
        }
    }

    /**
     * Deletes the book and the file based off its information.
     *
     * @param book - Book that'll be deleted
     * @return Mono<Boolean> determines whether or not book was successfully deleted
     * true - Book was deleted
     * false - Book wasn't deleted
     */
    @Override
    public Mono<Void> deleteBook(Book book) {
        String[] blobConversion = getBlobInformation(book.getAuthor(), book.getTitle());
        Mono<BlobItem> file = bookContainerClient.flatMapMany(containerAsyncClient
            -> containerAsyncClient.listBlobsFlat().filter(blobItem ->
            blobItem.name().contains(blobConversion[2] + "/"
                + blobConversion[1]
                + "/" + blobConversion[0]))).elementAt(0);
        return bookContainerClient.flatMap(containerAsyncClient ->
            file.flatMap(blobItem -> {
                final BlockBlobAsyncClient blob = containerAsyncClient.getBlockBlobAsyncClient(blobItem.name());
                return blob.delete().then();
            }));
      /*  return deleteImage(book).then(bookContainerClient.flatMap(containerAsyncClient -> {
                final BlockBlobAsyncClient blob = containerAsyncClient.getBlockBlobAsyncClient(blobConversion[2] + "/"
                    + blobConversion[1]
                    + "/" + blobConversion[0] + ".json");
                return blob.delete().then();
            }
        ));*/
    }

    private Mono<Void> deleteImage(Book book) {
        String[] blobConversion = getBlobInformation(book.getAuthor(), book.getTitle());
        Mono<BlobItem> file = imageContainerClient.flatMapMany(containerAsyncClient
            -> containerAsyncClient.listBlobsFlat().filter(blobItem ->
            blobItem.name().contains(blobConversion[2] + "/"
                + blobConversion[1]
                + "/" + blobConversion[0]))).elementAt(0);
        return imageContainerClient.flatMap(containerAsyncClient ->
            file.flatMap(blobItem -> {
                final BlockBlobAsyncClient blob = containerAsyncClient.getBlockBlobAsyncClient(blobItem.name());
                return blob.delete().then();
            }));
    }

    /**
     * Filters out the book based on the specified title.
     *
     * @param title - String of the book title the user is looking for
     * @return - Flux of Book objects with that title
     */
    @Override
    public Flux<Book> findBook(String title) {
        return getBooks().filter(book -> title.contentEquals(book.getTitle()));
    }

    /**
     * Filters out the books based on the specified author.
     *
     * @param author - Contains the name of the author the user is looking for
     * @return - Flux of Book objects by that author
     */
    @Override
    public Flux<Book> findBook(Author author) {
        return getBooks().filter(book -> author.getFirstName().contentEquals(book.getAuthor().getFirstName())
            && book.getAuthor().getLastName().contentEquals(author.getLastName()));
    }

    /**
     * Determines whether the collection has books or not.
     *
     * @return {@Link Mono<Boolean>} - true if there are books
     * false - if there are no books
     */
    @Override
    public Mono<Boolean> hasBooks() {
        return getBooks().hasElements();
    }

    /**
     * Converts the path to a file and then returns the URI to that path
     *
     * @param path - String containing the image file the user entered
     * @return URI of the image path
     */
    @Override
    public URI retrieveURI(String path) {
        return new File(path).toURI();
    }

    /**
     * Grab a String containing the absolute path to the book's cover location
     * If it's in Azure Database storage, the cover will be downloaded to the temporary directory.
     *
     * @param book - Book object of whose cover you want to retrieve
     * @return {@Link Mono} holds a String of the absolute path
     */
    @Override
    public Mono<String> grabCoverImage(Book book) {
        String[] blobConversion = getBlobInformation(book.getAuthor(), book.getTitle());
        Mono<BlobItem> file = imageContainerClient.flatMapMany(containerAsyncClient ->
            containerAsyncClient.listBlobsFlat().filter(blobItem -> blobItem.name().contains(blobConversion[2] + "/"
                + blobConversion[1]
                + "/" + blobConversion[0] + "."))).elementAt(0);
        return imageContainerClient.flatMap(containerAsyncClient ->
            file.flatMap(blobItem -> {
                final BlockBlobAsyncClient blockBlob = containerAsyncClient.getBlockBlobAsyncClient(blobItem.name());
                String property = "java.io.tmpdir";
                String tempDir = System.getProperty(property);
                File newFile = new File(tempDir + blobItem.name().
                    substring(blobItem.name().lastIndexOf("/") + 1));
                try {
                    newFile.createNewFile();
                } catch (IOException e) {
                    logger.error("Exception creating the file: ", e);
                    return Mono.error(e);
                }
                return blockBlob.downloadToFile(newFile.getAbsolutePath()).then(Mono.fromCallable(() ->
                    newFile.getAbsolutePath() + "\n\tThis was downloaded and saved to the user's TEMP folder."));
            }));
    }
}
