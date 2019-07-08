// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.app;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonHandlerTest {
    private JsonHandler jsonHandler = new JsonHandler();
    private URL folder = AppTest.class.getClassLoader().getResource(".");

    /**
     * Tests the JsonHandler class
     */
    @Test
    @Ignore("Needs update to use local paths")
    public void testSerializationAndCheckBook() {
        //Good book
        Book b = new Book("Wonder", new Author("Palacio", "R. J."),
            new File(folder.getPath() + "\\Book.png"));
        assertTrue(jsonHandler.writeJSON(b));
        //Bad book (empty title)
        Book b2 = new Book("", new Author("Palacio", "R. J."),
            new File(folder.getPath() + "\\Book.png")
        );
        assertFalse(jsonHandler.writeJSON(b2));
        //Bad book (Invalid author)
        Book b3 = new Book("Wonder", new Author("", null),
            new File(folder.getPath() + "\\Book.png"));
        assertFalse(jsonHandler.writeJSON(b3));
        //Bad book (Wrong file path)
        Book b4 = new Book("Wonder", new Author("Palacio", "R. J."),
            new File(""));
        assertFalse(jsonHandler.writeJSON(b4));
        //Completely bad book (wrong on all aspects)
        Book b5 = new Book(null, new Author(null, null),
            new File(""));
        assertFalse(jsonHandler.writeJSON(b5));
    }

    /**
     * Tests the deserialization of a JSON file to a book
     */
    @Test
    public void testFromJSONtoBook() {
        //Test with valid data
        Book result = jsonHandler.fromJSONtoBook(new File(folder.getPath()
            + "\\Kingdom Keepers VIII.json"));
        assertTrue(result != null);
        //Test with invalid data
        result = jsonHandler.fromJSONtoBook(new File(folder.getPath()
            + "//asdfasdf"));
        assertTrue(result == null);
    }
}
