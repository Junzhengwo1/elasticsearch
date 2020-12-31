package com.kou.pojo;

import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * @author JIAJUN KOU
 */

public class User implements Serializable {
    private String name;
    private Integer age;

    public User(String name, Integer age) {
    }

    public User() {
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
