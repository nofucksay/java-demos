package com.jyc.jdk8.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Created by jyc on 2017/10/23.
 */
public class JavaStream {


    public static void main(String[] args) {

    }

    private static List<Dog> createDogList() {
        List<Dog> dogs = new ArrayList<Dog>();
        dogs.add(new Dog("cawoo", "Beagle"));
        dogs.add(new Dog("blues", "Australian Shepherd"));
        dogs.add(new Dog("yahoo", "Beagle"));
        dogs.add(new Dog("akido", "Collie"));
        dogs.add(new Dog("subee", "Beagle"));
        return dogs;
    }


    @Test
    public void listing01() {
        // create dog list
        List<Dog> dogs = createDogList();

        // pick Beagle dogs into new list
        List<Dog> beagleDogs = new ArrayList<Dog>();
        for (Dog dog : dogs) {
            if ("Beagle".equals(dog.getBreed()))
                beagleDogs.add(dog);
        }

        // sort by name
        Collections.sort(beagleDogs, new Comparator<Dog>() {
            public int compare(Dog o1, Dog o2) {
                return o2.getName().compareTo(o1.getName());
            }
        });

        // take name into another list
        List<String> nameList = new ArrayList<String>();
        for (Dog beagleDog : beagleDogs) {
            nameList.add(beagleDog.getName());
        }

        // over print nameList
        System.out.println(nameList);

    }

    @Test
    public void listing02(){
        // create dog list
        List<Dog> dogs = createDogList();

        // implements by stream
        List<String> nameList = dogs.stream()
                .filter(e -> "Beagle".equals(e.getBreed()))
                .sorted(Comparator.comparing(Dog::getName).reversed())
                .map(Dog::getName)
                .collect(toList());

        // over print nameList
        System.out.println(nameList);
        
    }


    static class Dog {
        private String name;

        private String breed;

        public Dog(String name, String breed) {
            this.name = name;
            this.breed = breed;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getBreed() {
            return breed;
        }

        public void setBreed(String breed) {
            this.breed = breed;
        }

        @Override
        public String toString() {
            return String.format("[%s:%s]",getBreed(),getName());
        }
    }

}
