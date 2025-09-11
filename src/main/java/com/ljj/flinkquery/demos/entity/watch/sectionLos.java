package com.ljj.flinkquery.demos.entity.watch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class sectionLos {
    private String name;
    private String stake;
    private List<sectionLosDirPieces> dirlist;
}
