## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.

### 1.1.0

  - Allowed `kwargs` to be passed down to `PACDecorator`. Currently would only be used for defining the `episode_length` (number of days considered for post-acute-care)

### 1.0.0

  - Initial release of product component
  - Identify inpatient episodes and create claim decorators for anchor admissions, PAC major categories (IP, SNF, HH), PAC minor categories (IP, Rehab, Acute, Other), and transfers
