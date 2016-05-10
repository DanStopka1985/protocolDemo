package org.dao;

import org.config.Conf;
import org.ctrl.RestCtrl;
import org.entities.Author;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;
//import org.springframework.web.context.WebApplicationContext;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;


/**
 * Created by Stepan Danilov on 10.12.2015.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Conf.class/*, loader = AnnotationConfigWebContextLoader.class*/)
//@Transactional
//@Rollback(false)

public class AuthorDAOTest {

    @Autowired
    AuthorDAO authorDAO;

    @Autowired
    BookDAO bookDAO;


    @Test
    public void testGetAll() throws Exception {
        assertNotNull(authorDAO);

        List<Author> authorList = authorDAO.getAll();
        int authorsCnt = authorList.size();

        Author author = new Author();
        author.setName("test author");
        authorDAO.create(author);
        System.out.println(author.getId());
        authorList = authorDAO.getAll();
        assertEquals(authorsCnt + 1, authorList.size());

        int testAuthorId = author.getId();
        authorDAO.delete(testAuthorId);
        authorList = authorDAO.getAll();
        assertEquals(authorsCnt, authorList.size());

    }

}