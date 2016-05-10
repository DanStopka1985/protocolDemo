package org.dao;

import org.entities.A;
import org.entities.Author;
import org.entities.Book;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Service
@SuppressWarnings(value = "unchecked")
public class BookDAO {
    @Autowired
    private SessionFactory sessionFactory;

//    @Transactional(readOnly = true)
//    public List<Book> getAll(){
//
//        return sessionFactory.getCurrentSession().createSQLQuery("select 1 a").list();
//
//    }

    @Transactional(readOnly = true)
    public List<A> getById(int id){
        return (List<A>) sessionFactory.getCurrentSession().createSQLQuery("with t as (select 1 a union all select 2 a) select * from t").list();
    }

//    @Transactional
//    public void create(Book book){
//        Author author = book.getAuthor();
//        author = (Author) sessionFactory.getCurrentSession().get(Author.class, author.getId());
//        book.setAuthor(author);
//        sessionFactory.getCurrentSession().persist(book);
//    }
//
//    @Transactional
//    public void update(Book book){
//        sessionFactory.getCurrentSession().update(book);
//    }
//
//    @Transactional
//    public void delete(int id){
//        Book book = (Book) sessionFactory.getCurrentSession().get(Book.class, id);
//        if (book != null)
//            sessionFactory.getCurrentSession().delete(book);
//    }

}
