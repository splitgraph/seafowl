// This module contains various DataFusion traits/methods that are one of:
//   - private to DataFusion but we want to be able to use them because they do
//     exactly what we want
//   - part of DataFusion that we want to change but that doesn't have explicit extension
//     points, so it's easier to copy-paste the module and make our own alterations

pub mod parser;
pub mod utils;
