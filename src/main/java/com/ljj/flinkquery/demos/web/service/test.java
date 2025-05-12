package com.ljj.flinkquery.demos.web.service;

import com.ljj.flinkquery.demos.entity.GeoUtils;

public class test {

    public static void main(String[] args) {
        System.out.println(GeoUtils.MBR.hasIntersection(new GeoUtils.MBR(114.03852081298828, 114.04580688476562, 30.91611099243164, 30.919893264770508), new GeoUtils.MBR( 114.034,114.049, 30.924, 30.913)));
    }}
