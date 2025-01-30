package com.andrey;

import java.io.IOException;

public class MyKeyValueDB {
    public static void main(String[] args) {
        System.out.println("Starting MyKeyValueDB...");
        try {
            MyAPILayer apiLayer = new MyAPILayer(8080);
            apiLayer.start();
        } catch (IOException e) {
            System.err.println("Failed to start API layer: " + e.getMessage());
        }
    }
} 