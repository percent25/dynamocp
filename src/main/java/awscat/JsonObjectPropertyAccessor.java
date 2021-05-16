package awscat;

import java.math.BigDecimal;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;

public class JsonObjectPropertyAccessor implements PropertyAccessor {

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] {JsonObject.class};
	}

	@Override
	public boolean canRead(EvaluationContext context, @Nullable Object target, String name) throws AccessException {
		return true;
	}

	@Override
	public TypedValue read(EvaluationContext context, @Nullable Object target, String name) throws AccessException {
		JsonObject jsonObject = JsonObject.class.cast(target);
		JsonElement jsonElement = jsonObject.get(name);
		Object value = new Gson().fromJson(jsonElement, Object.class);

		// workaround for double/LazilyParsedNumber
		if (jsonElement.isJsonPrimitive()) {
			if (jsonElement.getAsJsonPrimitive().isNumber())
				value = new BigDecimal(jsonElement.getAsString());
		}

		return new TypedValue(value);
	}

	@Override
	public boolean canWrite(EvaluationContext context, @Nullable Object target, String name) throws AccessException {
		return true;
	}

	@Override
	public void write(EvaluationContext context, @Nullable Object target, String name, @Nullable Object newValue) throws AccessException {
		JsonObject jsonObject = JsonObject.class.cast(target);
		jsonObject.add(name, new Gson().toJsonTree(newValue));
	}

}
