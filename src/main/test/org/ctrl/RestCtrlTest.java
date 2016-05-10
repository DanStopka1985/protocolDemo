package org.ctrl;

import org.config.Conf;
import org.dao.AuthorDAO;
import org.entities.Author;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.mock.http.MockHttpOutputMessage;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * Created by Stepan Danilov on 14.12.2015.
 */

//@WebAppConfiguration

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Conf.class/*, loader = AnnotationConfigWebContextLoader.class*/)

public class RestCtrlTest {

    //private List<Author> authors = new ArrayList<>();


    @Autowired
    private AuthorDAO authorDAO;

    @Autowired
    private RestCtrl ctrl;

    private MockMvc mockMvc;

    @Before
    public void setup() {

        // Process mock annotations
        MockitoAnnotations.initMocks(this);

        // Setup Spring test in standalone mode
        this.mockMvc = MockMvcBuilders.standaloneSetup(ctrl).build();

    }

    @Test
    public void testCreateSignupFormInvalidUser() throws Exception {

        MediaType contentType = new MediaType(MediaType.APPLICATION_JSON.getType(),
                MediaType.APPLICATION_JSON.getSubtype(),
                Charset.forName("utf8"));

        Author author = new Author();
        author.setName("new author");
        authorDAO.create(author);

        mockMvc.perform(get("/authors"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(contentType)).andDo(print());


//        Author author = new Author();
//        author.setName("new author");
//        authorDAO.create(author);

        System.out.println("id: " + author.getId());
        System.out.println(authorDAO.getAll());
        mockMvc.perform(get("/author/" + author.getId()).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
                .andDo(print());
                //.andExpect(content().contentTypeCompatibleWith("application/json"));
                //.andExpect(jsonPath("$", hasSize(2)));

        //.andExpect(jsonPath("$[0].id", is(11)));








//        mockMvc.perform(get("/authors"))
//                .andExpect(status().is);
//
//        ResultActions resultActions = mockMvc.perform(get("/authors"));
//        resultActions.toString();
    }


}