# OT Platform. Statistic Commands Plugin

Additional commands for statistic work with data in OTL.

## Getting Started

You need published local dispatcher-sdk lib in order not to use unmanaged libs.

### Command List
#### rare - finds the least common values in the selected fields of event.
    | rare [limit] <field-list> [by-clause]

Arguments:
- limit - the number of top rows to be output in the result. The default is 10.
  If value of this argument is 0, than all rows will be output in the result.
- field-list - comma-separated field names to which the command will apply.
- by - defines the field by which grouping occurs in the table.

#### percentile - calculates the percentile of values of defined column.
    | percentile (<field> [as <resultfield>]) [value=double] [frequency=int] 

Arguments:
- field - name of field to which the command will apply.
- value - the value of percentage. Must be between 0.0 and 1.0. Default value: 1.0.
- frequency - the value of frequency for percentile. Must be positive integer number. Default value: 1.

#### percentile_approx - calculates the approximated percentile of values of defined column.
    | percentile_approx (<field> [as <resultfield>]) [value=double] [accuracy=int]

Arguments:
- field - name of field to which the command will apply.
- value - the value of percentage. Must be between 0.0 and 1.0. Default value: 1.0.
- accuracy - the value of accuracy for approximation. Must be positive integer number. Default value: 10000.

## Running the tests

sbt test

## Deployment

See Readme.md in root of Software Development Kit project.

## Dependencies

- dispatcher-sdk_2.11  1.2.1
- sbt 1.5.8
- scala 2.11.12

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository.

## Authors

Dmitriy Nikolaev (dnikolaev@isgneuro.com)

## License

[OT.PLATFORM. License agreement.](LICENSE.md)