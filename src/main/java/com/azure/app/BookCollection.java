// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.app;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;

interface BookCollection {

    /**
     * Reads all the JSON files and then saves their informations in new Book objects
     *
     * @return Flux<Book> the flux with all the book information </Book>
     */
    Flux<Book> registerBooks();

    /**
     * Saves the book to a JSON file.
     *
     * @param title  - String with the title of the book
     * @param author -String array with author's first and last name
     * @param path   - the File path
     * @param choice - String containing y/n about whether the user wants to save it or not
     */
    Mono<Boolean> saveBook(String title, Author author, File path, String choice);
}
