## Ανάπτυξη Λογισμικού για Πληροφοριακά Συστήματα (Κ23α)

[Ζαμπάτης Θεόδωρος](https://github.com/theo-zampatis) – 111520130045    
[Κωστακόντη Σοφία](https://github.com/SofiaKstk) – 1115201500080  
[Πελεκούδας Ιωάννης](https://github.com/pelekoudasq) – 1115201500128  

### Τελική Αναφορά

---

Στην εργασία αυτή καλούμαστε να υλοποιήσουμε τον αλγόριθμο ζεύξης πινάκων Radix Hash Join (RHS), ο οποίος κομματιάζει
τα δεδομένα σε κατάλληλο μέγεθος έτσι ώστε κατά την επεξεργασία τους να μπορούν να χωρέσουν στην cache μνήμη του επεξεργαστή.

Το πρόγραμμα εκτελείται απευθείας χρησιμοποιώντας τα δεδομένα που υπάρχουν στον κατάλογο small
```bash
> make run
```
ή χρησιμοποιόντας κάποια άλλα αρχεία εισόδου (ονόματος file στο παράδειγμα)
```bash
> cat file.init file.work | ./join
```
όπου κατά συνθήκη, η τελευταία γραμμή του αρχείου init είναι 'Done'.

Το πρόγραμμα, ενώ διαβάζει τα filepaths,
