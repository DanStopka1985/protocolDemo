package org.config;

import net.xqj.basex.BaseXXQDataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBuilder;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import java.util.Properties;

@Configuration
//@ContextConfiguration
//@WebAppConfiguration
@EnableTransactionManagement
@ComponentScan(basePackages = { "org.config", "org.dao", "org.ctrl" })
public class Conf {

//    @Bean
//    public TemplateResolver templateResolver(){
//        ServletContextTemplateResolver templateResolver = new ServletContextTemplateResolver();
//        templateResolver.setPrefix("/WEB-INF/templates/");
//        templateResolver.setSuffix(".html");
//        templateResolver.setTemplateMode("HTML5");
//
//        return templateResolver;
//    }

//    @Bean
//    public SpringTemplateEngine templateEngine(){
//        SpringTemplateEngine templateEngine = new SpringTemplateEngine();
//        templateEngine.setTemplateResolver(templateResolver());
//        return templateEngine;
//    }

//    @Bean
//    public ViewResolver viewResolver(){
//        ThymeleafViewResolver viewResolver = new ThymeleafViewResolver() ;
//        viewResolver.setTemplateEngine(templateEngine());
//        viewResolver.setOrder(1);
//
//        return viewResolver;
//    }

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

    @Bean(name = "baseXDataSource")
    public XQDataSource xqDataSource() throws XQException {
        XQDataSource ds = new BaseXXQDataSource();
        ds.setProperty("serverName", "localhost");
        ds.setProperty("port", "1984");
        ds.setProperty("user", "admin");
        ds.setProperty("password", "admin");
        ds.setProperty("databaseName", "2016");
        return ds;
    }

//    <beans profile="basex">
//    <bean id="xqDataSource" class="net.xqj.basex.BaseXXQDataSource">
//    <property name="user" value="admin" />
//    <property name="password" value="admin" />
//    <property name="serverName" value="localhost" />
//    <property name="port" value="1984" />
//    </bean>
//    </beans>

}
