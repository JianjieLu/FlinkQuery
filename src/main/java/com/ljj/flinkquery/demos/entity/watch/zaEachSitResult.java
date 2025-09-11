package com.ljj.flinkquery.demos.entity.watch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class zaEachSitResult {
    int code;
     String message;
     zaEachSitData data;
     boolean status;
}
