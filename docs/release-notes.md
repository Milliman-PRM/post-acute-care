## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.

### 1.3.0
  - Base `pac_claim_yn` off of `prm_fromdate_case` instead of `fromdate` for consistency across longer lengths of stay. 

### 1.2.0
  - Remove CCN/DRG override for PAC minor category

### 1.1.1

  - Fixed a bug involving duplicated lines for some cases which contained both `IP Acute` and `IP Rehab` categories

### 1.1.0

  - Allowed `kwargs` to be passed down to `PACDecorator`. Currently would only be used for defining the `episode_length` (number of days considered for post-acute-care)

### 1.0.0

  - Initial release of product component
  - Identify inpatient episodes and create claim decorators for anchor admissions, PAC major categories (IP, SNF, HH), PAC minor categories (IP, Rehab, Acute, Other), and transfers
