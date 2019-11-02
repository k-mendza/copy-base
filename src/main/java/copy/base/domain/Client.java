package copy.base.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Client {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;
}
