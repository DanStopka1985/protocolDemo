package org.temp;

/**
 * Created by RTLabs on 07.12.2015.
 */
public class Circle implements Figure{
    private double r;

    public double getR() {
        return r;
    }

    public void setR(double r) {
        this.r = r;
    }

    public double getSpace() {
        return Math.PI * r * r;
    }

    public double getPerimeter() {
        return 2 * Math.PI * r;
    }
}
