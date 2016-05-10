package org.config;

import org.apache.commons.dbcp.BasicDataSource;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBuilder;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.ViewResolver;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.spring4.view.ThymeleafViewResolver;
import org.thymeleaf.templateresolver.ServletContextTemplateResolver;
import org.thymeleaf.templateresolver.TemplateResolver;
import java.util.Properties;

@Configuration
//@ContextConfiguration
//@WebAppConfiguration
@EnableTransactionManagement
@ComponentScan(basePackages = { "org.config", "org.dao", "org.ctrl" })
public class Conf {

    @Bean
    public TemplateResolver templateResolver(){
        ServletContextTemplateResolver templateResolver = new ServletContextTemplateResolver();
        templateResolver.setPrefix("/WEB-INF/templates/");
        templateResolver.setSuffix(".html");
        templateResolver.setTemplateMode("HTML5");

        return templateResolver;
    }

    @Bean
    public SpringTemplateEngine templateEngine(){
        SpringTemplateEngine templateEngine = new SpringTemplateEngine();
        templateEngine.setTemplateResolver(templateResolver());
        return templateEngine;
    }

    @Bean
    public ViewResolver viewResolver(){
        ThymeleafViewResolver viewResolver = new ThymeleafViewResolver() ;
        viewResolver.setTemplateEngine(templateEngine());
        viewResolver.setOrder(1);

        return viewResolver;
    }

    @Bean(name = "dataSource")
    public BasicDataSource dataSource() {

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://localhost:5432/rmisdb");
        ds.setUsername("postgres");
        ds.setPassword("postgres");
        return ds;
    }

    @Bean
    public SessionFactory sessionFactory() {

        LocalSessionFactoryBuilder builder =
                new LocalSessionFactoryBuilder(dataSource());
        builder.scanPackages("org/entities")
                .addProperties(getHibernateProperties());

        return builder.buildSessionFactory();
    }

    private Properties getHibernateProperties() {
        Properties prop = new Properties();
        prop.put("hibernate.format_sql", "true");
        prop.put("hibernate.show_sql", "true");
        prop.put("hibernate.dialect",
                "org.hibernate.dialect.PostgreSQL82Dialect");

        return prop;
    }

    //Create a transaction manager
    @Bean
    public HibernateTransactionManager txManager() {
        return new HibernateTransactionManager(sessionFactory());
    }


}
