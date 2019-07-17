// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.app;

import com.azure.data.appconfiguration.ConfigurationAsyncClient;
import com.azure.data.appconfiguration.credentials.ConfigurationClientCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A library application that keeps track of books using Azure services.
 */
public class App {
    private static final int INVALID = -1;
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final AtomicReference<List<Book>> AR_REFERENCE = new AtomicReference<>();
    private static final OptionChecker OPTION_CHECKER = new OptionChecker();
    private static BookCollection bookCollector;
    private static Logger logger = LoggerFactory.getLogger(JsonHandler.class);

    /**
     * Starting point for the library application.
     *
     * @param args Arguments to the library program.
     */
    public static void main(String[] args) {
        String connectionString = System.getenv("AZURE_APPCONFIG");
        if (connectionString == null || connectionString.isEmpty()) {
            System.err.println("Environment variable AZURE_APPCONFIG is not set. Cannot connect to App Configuration."
                + " Please set it.");
            return;
        }
        ConfigurationAsyncClient client;
        try {
            client = ConfigurationAsyncClient.builder()
                .credentials(new ConfigurationClientCredentials(System.getenv("AZURE_APPCONFIG")))
                .build();
            client.getSetting("IMAGE_STORAGE_TYPE").subscribe(input -> {
                if (input.value().value().equalsIgnoreCase("Local")) {
                    bookCollector = new LocalBookCollector(System.getProperty("user.dir"));
                } else {
                    System.out.println("Sorry, but Blob Storage is not yet supported. Switching to Local.");
                    bookCollector = new LocalBookCollector(System.getProperty("user.dir"));
                }
            });
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            logger.error("Exception with App Configuration: ", e);
        }
        System.out.print("Welcome! ");
        int choice;
        do {
            showMenu();
            String option = SCANNER.nextLine();
            choice = OPTION_CHECKER.checkOption(option, 5);
            switch (choice) {
                case 1:
                    listBooks().block();
                    break;
                case 2:
                    addBook();
                    break;
                case 3:
                    bookCollector.hasBooks().subscribe(x -> {
                        if (x) {
                            findBook();
                        } else {
                            System.out.println("There are no books to find.");
                        }
                    });
                    break;
                case 4:
                    bookCollector.hasBooks().subscribe(x -> {
                        if (x) {
                            deleteBook().block();
                        } else {
                            System.out.println("There are no books to delete.");
                        }
                    });
                    break;
                case 5:
                    System.out.println("Goodbye.");
                    break;
                default:
                    System.out.println("Please try again.");
                    break;
            }
            System.out.println("------------------------------------------------");
        } while (choice != 5);
    }

    private static void showMenu() {
        System.out.println("Select one of the options below (1 - 5).");
        System.out.println("1. List books");
        System.out.println("2. Add a book");
        System.out.println("3. Find a book");
        System.out.println("4. Delete book");
        System.out.println("5. Quit");
    }

    private static Mono<Void> listBooks() {
        Flux<Book> book = bookCollector.getBooks();
        return book.collectList().map(list -> {
            if (list.isEmpty()) {
                AR_REFERENCE.set(Collections.emptyList());
                System.out.println("There are no books.");
                return list;
            }
            System.out.println("Here are all the books you have: ");
            AR_REFERENCE.set(list);
            for (int i = 0; i < list.size(); i++) {
                Book book1 = list.get(i);
                System.out.println(i + 1 + ". " + book1);
            }
            return list;
        }).then();
    }

    private static void addBook() {
        System.out.println("Please enter the following information:");
        String title;
        String author;
        URI path;
        String choice = "x";
        do {
            System.out.println("1. Title?");
            title = SCANNER.nextLine();
        } while (!OPTION_CHECKER.validateString(title));
        do {
            System.out.println("2. Author?");
            author = SCANNER.nextLine();
        } while (!OPTION_CHECKER.validateAuthor(author.split(" ")));
        String[] authorName = parseAuthorsName(author.split(" "));
        Author newAuthor = new Author(authorName[0], authorName[1]);
        do {
            System.out.println("3. Cover image? (Enter \"Q\" to return to menu.)");
            String filePath = SCANNER.nextLine();
            if (filePath.equalsIgnoreCase("Q")) {
                choice = "Q";
            }
            path = bookCollector.retrieveURI(filePath);
        } while (!choice.equalsIgnoreCase("q") && !OPTION_CHECKER.checkImage(System.getProperty("user.dir"), path));
        if (!choice.equalsIgnoreCase("q")) {
            do {
                System.out.println("4. Save? Enter 'Y' or 'N'.");
                choice = SCANNER.nextLine();
            } while (OPTION_CHECKER.checkYesOrNo(choice));
            if (choice.equalsIgnoreCase("y")) {
                bookCollector.saveBook(title, newAuthor, path).subscribe(x -> {
                    if (x) {
                        System.out.println("Book was successfully saved!");
                    } else {
                        System.out.println("Error. Book wasn't saved");
                    }
                });
            }
        }
    }

    private static void findBook() {
        int choice;
        System.out.println("How would you like to find the book? (Enter \"Q\" to return to menu.)");
        do {
            System.out.println("1. Search by book title?");
            System.out.println("2. Search by author?");
            String option = SCANNER.nextLine();
            choice = OPTION_CHECKER.checkOption(option, 2);
        } while (choice == INVALID);
        switch (choice) {
            case 0:
                break;
            case 1:
                findTitle().block();
                break;
            case 2:
                findAuthor().block();
                break;
            default:
                System.out.println("Please enter a number between 1 or 2.");
        }
    }

    private static Mono<Void> findTitle() {
        System.out.println("What is the book title?");
        String title = SCANNER.nextLine();
        Flux<Book> booksToFind = bookCollector.findBook(title);
        return booksToFind.collectList().map(list -> {
            if (list.isEmpty()) {
                System.out.println("There are no books with that title.");
            } else if (list.size() == 1) {
                System.out.println("Here is a book titled " + title + ".");
                System.out.println(" * " + list.get(0));
                System.out.println("Would you like to view it?");
                String choice = getYesOrNo();
                if (choice.equalsIgnoreCase("y")) {
                    System.out.println(list.get(0).displayBookInfo());
                }
            } else {
                System.out.println("Here are the books titled " + title + ". Please enter the number you wish to view. (Enter \"Q\" to return to menu.)");
                for (int i = 0; i < list.size(); i++) {
                    Book book1 = list.get(i);
                    System.out.println(i + 1 + ". " + book1);
                }
                int choice;
                do {
                    String option = SCANNER.nextLine();
                    choice = OPTION_CHECKER.checkOption(option, list.size());
                } while (choice == INVALID);
                if (choice != 0) {
                    System.out.println(list.get(choice - 1).displayBookInfo());
                }
            }
            return list;
        }).then();
    }

    private static Mono<Void> findAuthor() {
        System.out.println("What is the author's full name?");
        String author = SCANNER.nextLine();
        String[] name = parseAuthorsName(author.split(" "));
        Flux<Book> booksToFind = bookCollector.findBook(new Author(name[0], name[1]));
        return booksToFind.collectList().map(list -> {
            if (list.isEmpty()) {
                AR_REFERENCE.set(Collections.emptyList());
                System.out.println("There are no books by that author.");
                return list;
            }
            AR_REFERENCE.set(list);
            if (list.size() == 1) {
                System.out.println("Here is a book by " + author + ".");
                System.out.println(" * " + list.get(0));
                System.out.println("Would you like to view it?");
                String choice = getYesOrNo();
                if (choice.equalsIgnoreCase("y")) {
                    System.out.println(list.get(0).displayBookInfo());
                }
            } else {
                System.out.println("Here are books by " + author + ". Please enter the number you wish to view. (Enter \"Q\" to return to menu.)");
                for (int i = 0; i < list.size(); i++) {
                    System.out.println(i + 1 + ". " + list.get(i));
                }
                int choice;
                do {
                    String option = SCANNER.nextLine();
                    choice = OPTION_CHECKER.checkOption(option, list.size());
                } while (choice == INVALID);
                if (choice != 0) {
                    System.out.println(list.get(choice - 1).displayBookInfo());
                }
            }
            return list;
        }).then();
    }

    private static Mono<Void> deleteBook() {
        System.out.println("Enter the title of the book to delete: ");
        Flux<Book> booksToDelete = bookCollector.findBook(SCANNER.nextLine());
        return booksToDelete.collectList().map(list -> {
            if (list.isEmpty()) {
                AR_REFERENCE.set(Collections.emptyList());
                System.out.println("There are no books with that title.");
                return list;
            }
            AR_REFERENCE.set(list);
            if (list.size() == 1) {
                System.out.println("Here is a matching book.");
                System.out.println(" * " + list.get(0));
                System.out.print("Would you like to delete it? ");
                String choice = getYesOrNo();
                if (choice.equalsIgnoreCase("Y")) {
                    deleteBookHelper(list.get(0));
                }
            } else {
                System.out.println("Here are matching books. Enter the number to delete :  (Enter \"Q\" to return to menu.) ");
                for (int i = 0; i < list.size(); i++) {
                    System.out.println(i + 1 + ". " + list.get(i));
                }
                int choice;
                do {
                    String option = SCANNER.nextLine();
                    choice = OPTION_CHECKER.checkOption(option, list.size());
                } while (choice == INVALID);
                if (choice != 0) {
                    System.out.println("Delete \"" + list.get(choice - 1) + "\"? Enter Y or N.");
                    String delete = SCANNER.nextLine();
                    if (delete.equalsIgnoreCase("y")) {
                        deleteBookHelper(list.get(choice - 1));
                    }
                }
            }
            return list;
        }).then();
    }

    private static void deleteBookHelper(Book b) {
        if (b.checkBook(System.getProperty("user.dir"))) {
            bookCollector.deleteBook(b).subscribe(bookDeleted -> {
                if (bookDeleted) {
                    System.out.println("Book is deleted.");
                } else {
                    System.out.println("Error. Book wasn't deleted.");
                }
            });
        }
    }

    private static String getYesOrNo() {
        System.out.println("Enter Y or N.");
        String yesOrNo;
        do {
            yesOrNo = SCANNER.nextLine();
        } while (OPTION_CHECKER.checkYesOrNo(yesOrNo));
        return yesOrNo;
    }

    private static String[] parseAuthorsName(String[] author) {
        String lastName = author[author.length - 1];
        StringBuilder firstName = new StringBuilder(author[0]);
        for (int i = 1; i < author.length - 1; i++) {
            firstName.append(" ").append(author[i]);
        }
        return new String[]{firstName.toString(), lastName};
    }
}
